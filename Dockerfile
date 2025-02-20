FROM mambaorg/micromamba:1.5.1
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/project/environment.yml
COPY --chown=$MAMBA_USER:$MAMBA_USER pyproject.toml /tmp/project/pyproject.toml
COPY --chown=$MAMBA_USER:$MAMBA_USER bagitify /tmp/project/bagitify

RUN micromamba install -y -n base -f /tmp/project/environment.yml && \
    micromamba clean --all --yes

WORKDIR /srv/bagitify

ENV PYTHONUNBUFFERED=1
# so that `python -m` can find the module
ENV PYTHONPATH=/tmp/project

ENTRYPOINT ["micromamba", "run", "python", "-m", "bagitify.cli"]
