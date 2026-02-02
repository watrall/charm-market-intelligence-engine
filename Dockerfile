FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Make first run smoother for non-experts.
RUN python -m spacy download en_core_web_sm 2>/dev/null || true
RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)" 2>/dev/null || true

COPY . /app

EXPOSE 8501

RUN chmod +x /app/scripts/docker/entrypoint.sh
ENTRYPOINT ["/app/scripts/docker/entrypoint.sh"]
CMD ["dashboard"]

