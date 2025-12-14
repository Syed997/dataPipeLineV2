FROM flink:1.20.3

# System deps
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    bash \
    curl \
    git \
    vim \
    netcat \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Python symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# Python deps
RUN pip3 install --no-cache-dir \
    apache-flink==1.20.3 \
    py4j

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/project
ENV FLINK_PYTHON_EXECUTABLE=/usr/bin/python3
ENV FLINK_PYTHON_CLIENT_EXECUTABLE=/usr/bin/python3

# Dev workspace
COPY /flink /project

# Non-root user
RUN useradd -ms /bin/bash dev
USER dev

CMD ["bash"]