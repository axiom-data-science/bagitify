FROM mambaorg/micromamba:1.5.1
COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml

RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

WORKDIR /srv/bagitify

COPY ./bagitify.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "./bagitify.py"]
