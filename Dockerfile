# Use the official Python image corresponding to the project's version
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install uv, a fast Python package installer
RUN pip install uv

# Copy the dependency management files to the container
COPY pyproject.toml uv.lock ./

# Install the project dependencies using uv
# The --system flag installs packages into the global site-packages,
# which is a common practice in containers.
RUN uv pip sync --system

# Copy the rest of the application's source code
COPY . .

# Specify the command to run when the container starts
# This command runs the 'raw_message_processor' package as a script
CMD ["python", "-m", "raw_message_processor"]
