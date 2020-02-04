# An integration test & dev container based on rapids-dev-nightly with CLX installed from current branch
ARG RAPIDS_VERSION=0.12
ARG CUDA_VERSION=10.0
ARG LINUX_VERSION=ubuntu18.04
ARG PYTHON_VERSION=3.7

FROM rapidsai/rapidsai-dev-nightly:${RAPIDS_VERSION}-cuda${CUDA_VERSION}-devel-${LINUX_VERSION}-py${PYTHON_VERSION}

ADD . /rapids/clx/

SHELL ["/bin/bash", "-c"]
    source activate rapids \
    && conda install -c blazingsql-nightly/label/cuda${CUDA_VERSION} -c blazingsql-nightly -c rapidsai-nightly -c conda-forge -c defaults blazingsql \
    && pip install flatbuffers \

RUN source activate rapids \
    && conda install --freeze-installed panel=0.6.* datashader geopandas pyppeteer cuxfilter s3fs \
    && cd /rapids \
    && git clone https://github.com/rapidsai/cudatashader.git  /rapids/cudatashader \
    && cd /rapids/cudatashader \
    && pip install -e . \
    && cd /rapids/clx \
    && python setup.py install

WORKDIR /rapids
