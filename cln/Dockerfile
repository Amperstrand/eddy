# eddy-cln lab image: upstream CLN v26.06 + python plugin runtime.
# The stock image ships no python; pyln-client is all a plugin needs.
FROM elementsproject/lightningd:v26.06

USER root
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir --break-system-packages pyln-client
