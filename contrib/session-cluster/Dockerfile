# Append this stage to the root Dockerfile; see deploy/README.md.
# The root runtime remains the default image for upstream builds.
FROM hermes_runtime AS session_cluster
USER root
RUN uv pip install --python /opt/hermes/.venv/bin/python --no-deps /opt/hermes/contrib/session-cluster \
    && mkdir -p /data \
    && chown 10000:10000 /data
WORKDIR /data
ENV HOME=/data HERMES_HOME=/data/hermes
USER 10000:10000
ENTRYPOINT []
CMD ["python", "-m", "hermes_cluster", "--config", "/config/controller.json"]
