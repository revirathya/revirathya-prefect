FROM avidito/revirathya-toolbox:0.1.0

# Install dependencies
COPY README.md pyproject.toml uv.lock ./
RUN uv sync --frozen

ADD ./flows flows/