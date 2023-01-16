FROM python:3.6-stretch

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends \
      locales jq vim \
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

RUN pip install git+https://github.com/MtkN1/pybybit.git
COPY ./requirements.txt /requirements.txt
RUN pip install -r requirements.txt

COPY scripts /scripts
COPY entrypoint.sh /entrypoint.sh
WORKDIR /scripts

ENTRYPOINT ["/entrypoint.sh"]
