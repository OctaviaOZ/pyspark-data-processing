FROM bitnami/spark:3.5.0-debian-11-r0

# Install system dependencies for Poetry and Python build tools
USER root
RUN install_packages curl python3-pip build-essential && pip install poetry==1.4.2

# Add Poetry to the PATH
ENV PATH="/root/.local/bin:${PATH}"

# Set the working directory
WORKDIR /app

# Copy only dependency files first for better build cache usage
COPY pyproject.toml poetry.lock* ./

# Install Python dependencies
RUN poetry install --no-root --no-interaction --no-ansi

# Copy the rest of the application source code
COPY src ./src
COPY tests ./tests

# Create a non-root user and group (GID will be 1001)
# The GID is explicitly set to make it predictable.
RUN groupadd -r -g 1001 sparkgroup && useradd -r -u 1001 -g sparkgroup sparkuser

# Grant write permissions to the group for Spark's internal directories.
# This allows a container running with a different UID but the same GID to function.
RUN chown -R sparkuser:sparkgroup /opt/bitnami/spark

# Set ownership of the app directory
RUN chown -R sparkuser:sparkgroup /app

# Switch to the non-root user
USER sparkuser

# Specify the command for your Spark/Python app
CMD ["spark-submit", "--master", "local", "main.py"]
