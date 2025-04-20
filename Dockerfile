FROM avidito/revirathya-toolbox:0.1.0

# Install dependencies
COPY README.md pyproject.toml uv.lock ./
RUN uv pip install -r pyproject.toml

COPY shared/ shared/
COPY flows/ flows/