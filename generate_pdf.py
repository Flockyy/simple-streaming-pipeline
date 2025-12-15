from markdown2 import markdown
from weasyprint import HTML
import sys

# Read markdown
with open('veille_spark_streaming.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

# Convert to HTML
html_content = markdown(md_content, extras=['tables', 'fenced-code-blocks', 'code-friendly'])

# Add CSS styling
html_with_style = '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        @page {
            size: A4;
            margin: 2cm;
        }
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
        }
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
            page-break-after: avoid;
        }
        h2 {
            color: #34495e;
            border-bottom: 2px solid #95a5a6;
            padding-bottom: 8px;
            margin-top: 30px;
            page-break-after: avoid;
        }
        h3 {
            color: #7f8c8d;
            margin-top: 20px;
            page-break-after: avoid;
        }
        h4 {
            color: #95a5a6;
            margin-top: 15px;
            page-break-after: avoid;
        }
        code {
            background-color: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }
        pre {
            background-color: #2c3e50;
            color: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            page-break-inside: avoid;
            font-size: 0.85em;
            line-height: 1.4;
        }
        pre code {
            background-color: transparent;
            color: #ecf0f1;
            padding: 0;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
            page-break-inside: avoid;
        }
        th, td {
            border: 1px solid #bdc3c7;
            padding: 12px;
            text-align: left;
        }
        th {
            background-color: #3498db;
            color: white;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #ecf0f1;
        }
        blockquote {
            border-left: 4px solid #3498db;
            padding-left: 20px;
            color: #7f8c8d;
            margin: 20px 0;
        }
        ul, ol {
            margin: 10px 0;
            padding-left: 30px;
        }
        li {
            margin: 5px 0;
        }
        hr {
            border: none;
            border-top: 2px solid #bdc3c7;
            margin: 30px 0;
        }
        strong {
            color: #2c3e50;
        }
    </style>
</head>
<body>
''' + html_content + '''
</body>
</html>'''

# Generate PDF
HTML(string=html_with_style).write_pdf('veille_spark_streaming.pdf')
print('PDF generated successfully with proper formatting!')
