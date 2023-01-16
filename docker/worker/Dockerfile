FROM python:3.6-stretch

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends \
      locales jq vim less tmux \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* && \
    echo en_US.UTF-8 UTF-8 > /etc/locale.gen && \
    mkdir -p /usr/share/locale/ && \
    touch /usr/share/locale/locale.alias && \
    ln -snf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime && echo Asia/Tokyo > /etc/timezone && \
    locale-gen && \
    update-locale LANG=en_US.UTF-8 && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8

RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -zxvf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib && \
    ./configure --prefix=/usr && \
    make && make install

COPY ./requirements.txt /requirements.txt
RUN pip install -r requirements.txt
RUN pip install git+https://github.com/MtkN1/pybybit.git

# 起動スクリプトのコピー
COPY *.sh /
# 作業ディレクトリへ移動
WORKDIR /strategy

ENTRYPOINT ["/entrypoint.sh"]
