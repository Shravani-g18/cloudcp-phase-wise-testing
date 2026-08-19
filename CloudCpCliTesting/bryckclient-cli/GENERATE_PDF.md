# PDF Generation Guide

This directory contains an improved PDF generation workflow for OPERATIONS.md with better visual organization and formatting.

## Files

- **OPERATIONS.md** — Main documentation in Markdown format
  - Now includes YAML frontmatter for pandoc configuration
  - Contains explicit page breaks (`\newpage`) before major sections
  - Uses structured header hierarchy (## for major, ### for sub-sections)

- **style.tex** — LaTeX styling file for xelatex
  - Configures fonts, colors, and spacing
  - Adds header/footer with document title and page numbers
  - Defines callout box styles (Warning, Tip, Precondition)
  - Improves unicode character rendering (✓, ✗, ━, ─)
  - Enables syntax highlighting for code blocks
  - Configures margins and table formatting

- **generate_pdf.sh** — Automated PDF generation script
  - Reads OPERATIONS.md and style.tex
  - Runs pandoc with optimized settings
  - Outputs OPERATIONS.pdf with proper formatting

## Requirements

```bash
# Ubuntu/Debian
sudo apt-get install pandoc texlive-xetex texlive-latex-extra

# macOS (with Homebrew)
brew install pandoc
brew install --cask mactex

# Fedora/RHEL
sudo dnf install pandoc texlive-xetex texlive-latex
```

## Usage

### Automatic (using script)
```bash
cd bryckclient-cli/
chmod +x generate_pdf.sh
./generate_pdf.sh
# Output: OPERATIONS.pdf
```

### Manual (using pandoc directly)
```bash
pandoc OPERATIONS.md \
    --toc \
    --toc-depth=2 \
    --pdf-engine=xelatex \
    --listings \
    --include-in-header=style.tex \
    -V geometry:margin=1in \
    -V fontsize=11pt \
    -V monofont="DejaVu Sans Mono" \
    -V linkcolor:blue \
    -V urlcolor:blue \
    -o OPERATIONS.pdf
```

## PDF Features

### Table of Contents
- Auto-generated from markdown headers (## and ###)
- Clickable bookmarks in PDF reader
- Limited to 2-level depth for clarity (sections and subsections)

### Visual Organization
- **Page breaks** before major sections (1, 3, 5, 8, 10, 12) for easy navigation
- **Header/footer** with document title and page numbers
- **Proper spacing** between sections and paragraphs (1.15x line height)
- **Consistent fonts**:
  - Body: 11pt serif (default LaTeX font)
  - Code: 9pt monospace (DejaVu Sans Mono)
  - Tables: Proper alignment and spacing

### Code Blocks
- **Syntax highlighting** for supported languages (bash, json, python, etc.)
- **Line wrapping** for long lines
- **Framed** with borders for visual separation
- **Language detection** from markdown fence hints (```bash, ```json, etc.)

### Special Characters
- Unicode handling for ✓ (checkmark), ✗ (times), § (section), ━ (horizontal rule)
- Falls back to ASCII/LaTeX equivalents if needed
- Professional appearance in all PDF viewers

### Callout Boxes (future use)
- `{WarningBox}` — Orange border, yellow background (for critical warnings)
- `{TipBox}` — Blue border, light blue background (for tips)
- `{PreconditionBox}` — Red border, light red background (for state preconditions)

**Usage in markdown** (LaTeX blocks):
```latex
\begin{WarningBox}
Critical state precondition: Bryck must be in "Ejected" state
\end{WarningBox}
```

### Links and Colors
- **Blue clickable links** (TOC, cross-references, URLs)
- **Consistent color scheme** throughout document
- **No ugly boxes** around links (text color change only)

## Troubleshooting

### "Command not found: pandoc"
Install pandoc: `sudo apt-get install pandoc` (or use your package manager)

### "! LaTeX Error: File 'style.tex' not found"
Make sure `style.tex` is in the same directory as OPERATIONS.md

### Unicode characters appearing as boxes (✓ → ☐)
Update xelatex: `sudo apt-get install texlive-xetex`

### PDF looks different from previous version
This is expected! New PDF has:
- Auto-generated TOC with page numbers (instead of manual text list)
- Better page breaks and section separation
- Improved code block formatting with syntax highlighting
- Proper unicode character rendering
- Professional header/footer with page numbers

### PDF file is large (200+ KB)
This is normal! The new PDF includes:
- More detailed formatting (syntax highlighting, better tables)
- Auto-generated bookmarks
- Better compression via xelatex
- All original content plus improved visual structure

## Continuous Integration

To regenerate PDF automatically on changes:

```bash
# Watch for changes and regenerate
while inotifywait -e modify OPERATIONS.md; do
    ./generate_pdf.sh
done
```

Or use a Makefile:
```makefile
.PHONY: pdf
pdf: OPERATIONS.pdf

OPERATIONS.pdf: OPERATIONS.md style.tex
	./generate_pdf.sh

.PHONY: pdf-watch
pdf-watch:
	@while true; do \
		inotifywait -e modify OPERATIONS.md && make pdf; \
	done
```

## Version History

- **v2.0** (Aug 8, 2026): Improved visual organization
  - Added YAML frontmatter for pandoc metadata
  - Created style.tex for LaTeX customization
  - Added explicit page breaks before major sections
  - Auto-generated TOC with bookmarks
  - Syntax highlighting for code blocks
  - Better unicode character handling

- **v1.0** (Earlier): Basic pandoc PDF generation

---

**Questions?** Check OPERATIONS.md §12 (Technical Specifications) for pandoc and xelatex details.
