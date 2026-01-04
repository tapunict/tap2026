FROM apache/spark

USER root

# Setup Hugging Face libraries
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    transformers \
    torch --extra-index-url https://download.pytorch.org/whl/cpu

# Create cache directories for Hugging Face models
RUN mkdir -p /opt/spark/.cache/huggingface && \
    chown -R spark:spark /opt/spark/.cache

# Set environment variable for Hugging Face cache
ENV HF_HOME=/opt/spark/.cache/huggingface

USER spark
