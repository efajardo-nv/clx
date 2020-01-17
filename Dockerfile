ARG image=rapidsai/rapidsai-dev-nightly:0.12-cuda10.0-devel-ubuntu18.04-py3.7

FROM ${image}

ADD . /rapids/clx/

SHELL ["/bin/bash", "-c"]

RUN source activate rapids \
    && conda install -c blazingsql-nightly/label/cuda10.0 -c blazingsql-nightly -c rapidsai-nightly -c conda-forge -c defaults blazingsql \
    && pip install flatbuffers

RUN source activate rapids \
    && conda install --freeze-installed panel=0.6.* datashader geopandas pyppeteer cuxfilter s3fs \
    && cd /rapids \
    && git clone https://github.com/rapidsai/cudatashader.git  /rapids/cudatashader \
    && cd /rapids/cudatashader \
    && pip install -e . \
    && cd /rapids/clx \
    && python setup.py install

WORKDIR /rapids

