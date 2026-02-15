# Use Python slim image to keep it light
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Default command: Run the simulator and generate the PDF
CMD ["sh", "-c", "python3 scripts/simulator.py && python3 scripts/generate_pdf.py && echo 'Report Generated!'"]
