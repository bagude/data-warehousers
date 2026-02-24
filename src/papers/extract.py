"""Extract structured metadata and technical notes from SPE/URTeC PDFs.

Reads the full document text, extracts figures as images for later vLLM
processing, and uses a local Ollama model to generate metadata + dense
technical notes optimized for grep-based RAG retrieval.

Each paper gets its own directory under --output-dir:
    docs/paper_data/{paper_stem}/metadata.json
    docs/paper_data/{paper_stem}/figures/fig_p1_0.png

Usage:
    # Single paper
    python docs/extract_metadata.py --input docs/papers/some_paper.pdf

    # All papers (skip scanned PDFs without text layers)
    python docs/extract_metadata.py --input docs/papers/ --skip-ocr

    # Custom output directory
    python docs/extract_metadata.py --input docs/papers/ --output-dir docs/my_output
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import fitz  # pymupdf
import requests

# Suppress OpenMP duplicate library warnings (torch + numpy conflict on Windows)
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")

# Fix Windows cp1252 encoding for EasyOCR progress bar (█ character)
if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Lazy-loaded EasyOCR reader
_ocr_reader = None


def _get_ocr_reader():
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr
        _ocr_reader = easyocr.Reader(["en"], gpu=False)
    return _ocr_reader


# ── PDF text and image extraction ────────────────────────────────

def extract_text_from_pdf(pdf_path: str, max_pages: int = None, use_ocr: bool = True) -> str:
    """Extract text from all pages. Falls back to OCR for scanned PDFs."""
    doc = fitz.open(pdf_path)
    pages = min(len(doc), max_pages) if max_pages else len(doc)

    text = ""
    for i in range(pages):
        text += doc[i].get_text()

    if text.strip():
        doc.close()
        return text

    if not use_ocr:
        doc.close()
        return ""

    import numpy as np
    reader = _get_ocr_reader()
    ocr_texts = []
    for i in range(pages):
        pix = doc[i].get_pixmap(dpi=200)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        results = reader.readtext(img, detail=0, paragraph=True)
        ocr_texts.append("\n".join(results))

    doc.close()
    return "\n\n".join(ocr_texts)


# Minimum dimensions for a "real" figure (skip icons, decorations, logos)
_MIN_FIG_WIDTH = 200
_MIN_FIG_HEIGHT = 200


def extract_figures(pdf_path: str, output_dir: str) -> list[dict]:
    """Extract figures/graphs from PDF as PNG images.

    Returns list of {"page": int, "index": int, "path": str, "width": int, "height": int}.
    Images smaller than 200x200 are skipped (decorations/logos).
    """
    doc = fitz.open(pdf_path)
    figures = []
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    for page_num in range(len(doc)):
        images = doc[page_num].get_images(full=True)
        fig_idx = 0
        for img_info in images:
            xref, _, w, h = img_info[0], img_info[1], img_info[2], img_info[3]
            if w < _MIN_FIG_WIDTH or h < _MIN_FIG_HEIGHT:
                continue

            try:
                pix = fitz.Pixmap(doc, xref)
                # Convert CMYK or other colorspaces to RGB
                if pix.n > 4:
                    pix = fitz.Pixmap(fitz.csRGB, pix)
                elif pix.n == 4:
                    pix = fitz.Pixmap(fitz.csRGB, pix)

                fname = f"fig_p{page_num + 1}_{fig_idx}.png"
                out_path = str(Path(output_dir) / fname)
                pix.save(out_path)

                figures.append({
                    "page": page_num + 1,
                    "index": fig_idx,
                    "path": out_path,
                    "width": w,
                    "height": h,
                })
                fig_idx += 1
            except Exception:
                continue

    doc.close()
    return figures


# ── Preprocessing ────────────────────────────────────────────────

_LIGATURES = {
    "\ufb01": "fi", "\ufb02": "fl", "\ufb00": "ff",
    "\ufb03": "ffi", "\ufb04": "ffl",
    "\u2013": "-", "\u2014": "--", "\u2018": "'", "\u2019": "'",
    "\u201c": '"', "\u201d": '"',
}

_BOILERPLATE_PATTERNS = [
    re.compile(r"This paper was (prepared|selected) for presentation.*?(?=\n\n|\nAbstract|\nSummary)", re.DOTALL | re.IGNORECASE),
    re.compile(r"Contents of the paper.*?correction by the author\(s\)\.?", re.DOTALL | re.IGNORECASE),
    re.compile(r"The material.*?does not necessarily reflect.*?(?=\n\n)", re.DOTALL | re.IGNORECASE),
    re.compile(r"Permission to copy is restricted.*?(?=\n\n)", re.DOTALL | re.IGNORECASE),
]


def parse_filename(filename: str) -> dict:
    """Extract SPE/URTeC number, author, and topic hint from the filename."""
    hints = {"paper_number": None, "author_hint": None, "topic_hint": None}

    m = re.match(
        r'(SPE|URTeC)[_\s]*(?:\d{4}[_\s]+)?(\d+)[_\s]*(?:_(?:PA|MS|Part_\d+))?[_\s]*\(([^)]+)\)[_\s]*(.*?)(?:\s*\((?:pdf|wPres|wPes|preprint)\))?\.pdf$',
        filename, re.IGNORECASE
    )
    if m:
        prefix, number, author, topic = m.groups()
        number = number.lstrip("0") or "0"
        hints["paper_number"] = f"{prefix.upper()} {number}" if prefix.upper() == "SPE" else f"{prefix}: {number}"
        hints["author_hint"] = author.strip()
        topic = topic.replace("_", " ").replace("%20", " ").strip()
        topic = re.sub(r"\s*\((?:wPres|wPes|pdf|preprint)\)\s*", "", topic, flags=re.IGNORECASE).strip()
        hints["topic_hint"] = topic if topic else None

    return hints


def clean_text(text: str) -> str:
    """Clean PDF-extracted text for better LLM comprehension."""
    for old, new in _LIGATURES.items():
        text = text.replace(old, new)

    for pattern in _BOILERPLATE_PATTERNS:
        text = pattern.sub("", text)

    cleaned_lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            cleaned_lines.append("")
            continue
        alpha_ratio = sum(c.isalpha() or c.isspace() for c in stripped) / len(stripped)
        if alpha_ratio > 0.5 or len(stripped) < 5:
            cleaned_lines.append(stripped)

    text = "\n".join(cleaned_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ── LLM extraction ───────────────────────────────────────────────

EXTRACTION_PROMPT = r"""You are a petroleum engineering expert and data architect reading an SPE/URTeC technical paper.
Extract the FULL paper text below into an atomic, relational knowledge graph.

{hints_block}

Return a single JSON object containing arrays for each node type. Use sequential IDs (e.g., "eq_01", "method_02") so you can create edges between them. The document ALWAYS has node_id "doc_01".

1. "document": Object containing:
   - "node_id": "doc_01" (always this value)
   - "paper_number": string (e.g., "SPE 125031")
   - "title": string
   - "year": string
   - "abstract": string
   - "proposition": string (1-2 sentence hyper-dense technical summary)
   - "references": list of strings (SPE/URTeC numbers only)

2. "authors": List of objects:
   - "node_id": string (e.g., "author_01")
   - "name": string
   - "affiliation": string

3. "methods": List of objects:
   - "node_id": string (e.g., "method_01")
   - "name": string
   - "role": "proposed" | "extended" | "baseline_comparison" | "validation" | "applied" | "critiqued"
   - "context": string
   - "constraints": object with "flow_regime" (list), "fluid" (list), "pressure_condition" (string), "reservoir_type" (list), "is_multiphase_valid" (boolean)
   - "edges": object containing lists of node_ids (keys: "extends", "depends_on", "conflicts_with")
   - "confidence": float (0.0 to 1.0)

4. "equations": List of objects:
   - "node_id": string (e.g., "eq_01")
   - "name": string
   - "formula_latex": string (MUST be pure LaTeX)
   - "variables": dictionary mapping LaTeX symbols to their definition AND units in brackets (e.g., {{"q": "flow rate [Mscf/D]"}})
   - "constraints": object (same structure as methods)
   - "edges": object (keys: "derives_from", "depends_on")
   - "confidence": float (0.0 to 1.0)

5. "claims": List of objects (dense, standalone factual assertions):
   - "node_id": string (e.g., "claim_01")
   - "text": string (packed with specifics, numbers, and terminology)
   - "claim_type": "finding" | "assumption" | "limitation" | "recommendation" | "application" | "comparison" | "validation"
   - "section": string
   - "page": int (approximate)
   - "edges": object (keys: "supports", "derives_from", "conflicts_with")
   - "confidence": float (0.0 to 1.0)

6. "field_cases": List of objects (physical or simulated datasets):
   - "node_id": string (e.g., "data_01")
   - "name": string
   - "case_type": "field" | "simulation" | "experimental"
   - "basin": string or null
   - "formation": string or null
   - "reservoir_properties": dictionary of numeric values with unit-suffixed keys (e.g., {{"k_md": 0.012, "pi_psia": 14000}})
   - "edges": object (keys: "validates", "uses")
   - "confidence": float (0.0 to 1.0)

If a field is not found, use null for scalars or [] for lists.
Ensure edge references match the assigned node_ids perfectly.

Paper text:
{text}"""


logger = logging.getLogger(__name__)


def _normalize_paper_key(paper_number: str) -> str:
    """Strip non-alphanumeric chars and lowercase.
    'SPE 125031' -> 'spe125031', 'SPE-125031-MS' -> 'spe125031ms'
    """
    return re.sub(r'[^a-zA-Z0-9]', '', paper_number).lower()


def _globalize_id(local_id: str, paper_key: str) -> str:
    """Convert LLM-assigned local ID to global ID.
    'doc_01' -> 'doc_spe125031' (doc drops suffix)
    'eq_01' -> 'eq_spe125031_01'
    """
    prefix = local_id.split("_")[0]
    suffix = local_id.split("_")[-1]
    if prefix == "doc":
        return f"doc_{paper_key}"
    return f"{prefix}_{paper_key}_{suffix}"


def _atomize(parsed: dict, stem: str) -> list[dict]:
    """Transform LLM JSON response into atomic node dicts.

    Each node gets: node_id, node_type, parent_doc_id, domain_tags,
    page, confidence, content, edges.
    """
    doc_obj = parsed.get("document", {})
    paper_num = doc_obj.get("paper_number", stem)
    paper_key = _normalize_paper_key(paper_num)
    doc_id = f"doc_{paper_key}"

    # Build local->global ID map
    id_map = {}
    doc_local = doc_obj.get("node_id", "doc_01")
    id_map[doc_local] = doc_id

    for array_key in ("authors", "equations", "methods", "claims", "field_cases"):
        for item in parsed.get(array_key, []):
            local_id = item.get("node_id", "")
            if local_id:
                id_map[local_id] = _globalize_id(local_id, paper_key)

    def _resolve_edges(edges: dict, global_id: str) -> dict:
        resolved = {}
        for edge_type, targets in edges.items():
            valid = []
            for tid in targets:
                if tid in id_map:
                    valid.append(id_map[tid])
                else:
                    logger.warning(
                        "Dangling edge in %s: %s -> '%s' (unresolved)",
                        global_id, edge_type, tid
                    )
            resolved[edge_type] = valid
        return resolved

    nodes = []

    # Document node
    doc_content_keys = ("paper_number", "title", "year", "abstract", "proposition", "references")
    nodes.append({
        "node_id": doc_id,
        "node_type": "document",
        "parent_doc_id": doc_id,
        "domain_tags": doc_obj.get("domain_tags", []),
        "page": None,
        "confidence": doc_obj.get("confidence", 1.0),
        "content": {k: doc_obj.get(k) for k in doc_content_keys},
        "edges": {},
    })

    # Author nodes
    for item in parsed.get("authors", []):
        global_id = id_map.get(item.get("node_id", ""), f"author_{paper_key}_gen")
        nodes.append({
            "node_id": global_id,
            "node_type": "author",
            "parent_doc_id": doc_id,
            "domain_tags": [],
            "page": None,
            "confidence": 1.0,
            "content": {"name": item.get("name"), "affiliation": item.get("affiliation")},
            "edges": {},
        })

    # Equation nodes
    eq_content_keys = ("name", "formula_latex", "variables", "constraints")
    for item in parsed.get("equations", []):
        global_id = id_map.get(item.get("node_id", ""), f"eq_{paper_key}_gen")
        nodes.append({
            "node_id": global_id,
            "node_type": "equation",
            "parent_doc_id": doc_id,
            "domain_tags": item.get("domain_tags", []),
            "page": item.get("page"),
            "confidence": item.get("confidence", 0.5),
            "content": {k: item.get(k) for k in eq_content_keys},
            "edges": _resolve_edges(item.get("edges", {}), global_id),
        })

    # Method nodes
    method_content_keys = ("name", "role", "context", "applicable_reservoirs", "constraints")
    for item in parsed.get("methods", []):
        global_id = id_map.get(item.get("node_id", ""), f"method_{paper_key}_gen")
        nodes.append({
            "node_id": global_id,
            "node_type": "method",
            "parent_doc_id": doc_id,
            "domain_tags": item.get("domain_tags", []),
            "page": item.get("page"),
            "confidence": item.get("confidence", 0.5),
            "content": {k: item.get(k) for k in method_content_keys},
            "edges": _resolve_edges(item.get("edges", {}), global_id),
        })

    # Claim nodes
    claim_content_keys = ("text", "claim_type", "section")
    for item in parsed.get("claims", []):
        global_id = id_map.get(item.get("node_id", ""), f"claim_{paper_key}_gen")
        nodes.append({
            "node_id": global_id,
            "node_type": "claim",
            "parent_doc_id": doc_id,
            "domain_tags": item.get("domain_tags", []),
            "page": item.get("page"),
            "confidence": item.get("confidence", 0.5),
            "content": {k: item.get(k) for k in claim_content_keys},
            "edges": _resolve_edges(item.get("edges", {}), global_id),
        })

    # Field case nodes
    fc_content_keys = ("name", "case_type", "formation", "basin", "reservoir_properties", "results")
    for item in parsed.get("field_cases", []):
        global_id = id_map.get(item.get("node_id", ""), f"data_{paper_key}_gen")
        nodes.append({
            "node_id": global_id,
            "node_type": "field_case",
            "parent_doc_id": doc_id,
            "domain_tags": item.get("domain_tags", []),
            "page": item.get("page"),
            "confidence": item.get("confidence", 0.5),
            "content": {k: item.get(k) for k in fc_content_keys},
            "edges": _resolve_edges(item.get("edges", {}), global_id),
        })

    return nodes


def _build_prompt(text: str, hints: dict, backend: str = "ollama") -> str:
    """Build the extraction prompt with filename hints."""
    hint_lines = []
    if hints.get("paper_number"):
        hint_lines.append(f"Paper number (from filename): {hints['paper_number']}")
    if hints.get("author_hint"):
        hint_lines.append(f"Primary author (from filename): {hints['author_hint']}")
    if hints.get("topic_hint"):
        hint_lines.append(f"Topic hint (from filename): {hints['topic_hint']}")

    if hint_lines:
        hints_block = "The filename provides these hints (use them but verify against the text):\n" + \
                       "\n".join(f"  - {h}" for h in hint_lines)
    else:
        hints_block = ""

    if backend == "gemini":
        return EXTRACTION_PROMPT.format(
            hints_block=hints_block,
            text="[See attached PDF. Read the entire document including all figures, tables, and equations.]"
        )
    return EXTRACTION_PROMPT.format(hints_block=hints_block, text=text)


# ── Gemini backend ──────────────────────────────────────────────

def _call_gemini(pdf_path: str, prompt: str, api_key: str, model_id: str,
                 max_retries: int = 5) -> dict:
    """Send raw PDF to Gemini API with retry + exponential backoff for rate limits."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)
    pdf_bytes = open(pdf_path, "rb").read()

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=model_id,
                contents=[
                    types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
                    prompt,
                ],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                ),
            )
            return json.loads(response.text)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                # Parse retry delay from error if available
                retry_match = re.search(r"retryDelay.*?(\d+)", err_str)
                base_wait = int(retry_match.group(1)) if retry_match else 15
                wait = base_wait + (2 ** attempt * 5)
                print(f"    rate limited, waiting {wait}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Gemini rate limited after {max_retries} retries")


def extract_metadata(pdf_path: str, model_id: str, model_url: str, use_ocr: bool = True,
                     output_dir: str = None, backend: str = "ollama",
                     api_key: str = None) -> dict:
    """Extract metadata, technical notes, and figures from a PDF.

    If output_dir is provided, creates a per-paper directory:
        {output_dir}/{stem}/metadata.json
        {output_dir}/{stem}/figures/fig_p1_0.png

    backend: "ollama" (local, text-only) or "gemini" (API, native PDF vision)
    """
    filename = Path(pdf_path).name
    stem = Path(pdf_path).stem

    # Per-paper output directory
    paper_dir = None
    if output_dir:
        paper_dir = str(Path(output_dir) / stem)
        Path(paper_dir).mkdir(parents=True, exist_ok=True)

    # 1. Extract figures into per-paper figures/ subdirectory (always)
    figures = []
    if paper_dir:
        fig_dir = str(Path(paper_dir) / "figures")
        figures = extract_figures(pdf_path, fig_dir)

    # 2. Build prompt with filename hints
    hints = parse_filename(filename)

    if backend == "gemini":
        # Gemini: send raw PDF, no text extraction needed
        prompt = _build_prompt("", hints, backend="gemini")
        for attempt in range(2):
            try:
                parsed = _call_gemini(pdf_path, prompt, api_key, model_id)
                break
            except Exception as e:
                if attempt == 0:
                    continue
                return {"file": filename, "error": f"Gemini extraction failed: {e}"}
    else:
        # Ollama: extract text first
        text = extract_text_from_pdf(pdf_path, use_ocr=use_ocr)
        if not text.strip():
            return {"file": filename, "error": "No text extracted (scanned PDF)"}

        text = clean_text(text)
        prompt = _build_prompt(text[:100000], hints)

        api_url = model_url.rstrip("/") + "/api/generate"
        for attempt in range(2):
            try:
                resp = requests.post(api_url, json={
                    "model": model_id,
                    "prompt": prompt,
                    "format": "json",
                    "stream": False,
                    "options": {"temperature": 0.1, "num_ctx": 131072},
                }, timeout=600)
                resp.raise_for_status()
                raw = resp.json().get("response", "")
                if not raw.strip():
                    if attempt == 0:
                        continue
                    return {"file": filename, "error": "Model returned empty response"}
                parsed = json.loads(raw)
                break
            except (json.JSONDecodeError, requests.RequestException) as e:
                if attempt == 0:
                    continue
                return {"file": filename, "error": f"extraction failed: {e}"}

    # 3. Atomize into nodes
    nodes = _atomize(parsed, stem)

    # Add figure nodes
    for fig in figures:
        fig_id = f"fig_{_normalize_paper_key(parsed.get('document', {}).get('paper_number', stem))}_{fig['page']}_{fig['index']}"
        # Make path relative to paper dir
        fig_path = fig.get("path", "")
        if paper_dir and fig_path.startswith(paper_dir):
            fig_path = os.path.relpath(fig_path, paper_dir)
        nodes.append({
            "node_id": fig_id,
            "node_type": "figure",
            "parent_doc_id": nodes[0]["node_id"] if nodes else f"doc_{stem}",
            "domain_tags": [],
            "page": fig.get("page"),
            "confidence": 1.0,
            "content": {
                "path": fig_path.replace("\\", "/"),
                "width": fig.get("width"),
                "height": fig.get("height"),
            },
            "edges": {},
        })

    # 4. Save nodes.jsonl
    if paper_dir:
        nodes_path = os.path.join(paper_dir, "nodes.jsonl")
        with open(nodes_path, "w", encoding="utf-8") as f:
            for node in nodes:
                f.write(json.dumps(node, ensure_ascii=False) + "\n")

    return {"file": filename, "backend": backend, "model": model_id, "node_count": len(nodes)}


# ── Batch processing ─────────────────────────────────────────────

def process_directory(papers_dir: str, output_dir: str, model_id: str, model_url: str,
                      use_ocr: bool = True, backend: str = "ollama", api_key: str = None,
                      skip_existing: bool = False):
    """Process all PDFs in a directory. Each paper gets its own subdirectory."""
    papers = sorted(Path(papers_dir).glob("*.pdf"))
    if not papers:
        print(f"No PDFs found in {papers_dir}")
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Found {len(papers)} PDFs in {papers_dir}")
    print(f"Backend: {backend} | Model: {model_id}")
    print(f"Output: {output_dir}/{{paper_stem}}/nodes.jsonl")
    results = []

    for i, pdf in enumerate(papers, 1):
        # Skip if already successfully extracted
        if skip_existing:
            existing = Path(output_dir) / pdf.stem / "nodes.jsonl"
            if existing.exists():
                try:
                    first_line = existing.read_text(encoding="utf-8").split("\n")[0].strip()
                    if first_line:
                        node = json.loads(first_line)
                        if "node_type" in node:
                            print(f"[{i}/{len(papers)}] {pdf.name} -> SKIP (exists)")
                            results.append({"file": pdf.name, "node_count": 0, "skipped": True})
                            continue
                except (json.JSONDecodeError, OSError):
                    pass  # Re-extract corrupt files

        print(f"[{i}/{len(papers)}] {pdf.name}")
        t0 = time.time()
        try:
            meta = extract_metadata(str(pdf), model_id, model_url, use_ocr=use_ocr,
                                    output_dir=output_dir, backend=backend, api_key=api_key)
            results.append(meta)
            dt = time.time() - t0
            if "error" in meta:
                print(f"  -> SKIPPED: {meta['error']}")
            else:
                n_nodes = meta.get("node_count", 0)
                print(f"  -> {n_nodes} nodes | {dt:.0f}s")
        except Exception as e:
            error_msg = str(e).encode("ascii", errors="replace").decode()
            results.append({"file": pdf.name, "error": error_msg})
            print(f"  -> ERROR: {error_msg}")

        # Rate limit for Gemini free tier (5 RPM for Pro)
        if backend == "gemini" and i < len(papers):
            time.sleep(12)

    succeeded = sum(1 for r in results if "error" not in r and not r.get("skipped"))
    skipped = sum(1 for r in results if r.get("skipped"))
    failed = sum(1 for r in results if "error" in r)
    total_nodes = sum(r.get("node_count", 0) for r in results)
    print(f"\nDone: {succeeded} extracted, {skipped} skipped, {failed} failed out of {len(results)} papers")
    print(f"Total: {total_nodes} nodes")


def main():
    parser = argparse.ArgumentParser(description="Extract metadata and technical notes from SPE/URTeC PDFs")
    parser.add_argument("--input", default="docs/papers", help="PDF file or directory of PDFs")
    parser.add_argument("--output-dir", default="docs/paper_data", help="Output directory (each paper gets a subdirectory)")
    parser.add_argument("--backend", default="ollama", choices=["ollama", "gemini"], help="LLM backend")
    parser.add_argument("--model", default=None, help="Model ID (default: gemma3:12b for ollama, gemini-2.5-pro for gemini)")
    parser.add_argument("--model-url", default="http://localhost:11434", help="Ollama API URL")
    parser.add_argument("--api-key", default=None, help="API key for Gemini (or set GEMINI_API_KEY env var)")
    parser.add_argument("--skip-ocr", action="store_true", help="Skip scanned PDFs instead of running OCR (ollama only)")
    parser.add_argument("--skip-existing", action="store_true", help="Skip papers that already have a valid nodes.jsonl")
    args = parser.parse_args()

    # Defaults per backend
    if args.model is None:
        args.model = "gemini-2.5-pro" if args.backend == "gemini" else "gemma3:12b"

    api_key = args.api_key or os.environ.get("GEMINI_API_KEY")
    if args.backend == "gemini" and not api_key:
        print("Error: --api-key or GEMINI_API_KEY env var required for gemini backend", file=sys.stderr)
        sys.exit(1)

    input_path = Path(args.input)
    use_ocr = not args.skip_ocr

    if input_path.is_file():
        meta = extract_metadata(str(input_path), args.model, args.model_url,
                                use_ocr=use_ocr, output_dir=args.output_dir,
                                backend=args.backend, api_key=api_key)
        print(json.dumps(meta, indent=2, ensure_ascii=False))
    elif input_path.is_dir():
        process_directory(str(input_path), args.output_dir, args.model, args.model_url,
                          use_ocr=use_ocr, backend=args.backend, api_key=api_key,
                          skip_existing=args.skip_existing)
    else:
        print(f"Error: {args.input} is not a file or directory", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
