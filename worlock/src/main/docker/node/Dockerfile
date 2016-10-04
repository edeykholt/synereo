FROM synereo/base

RUN git clone --depth 1 https://github.com/synereo/synereo.git

WORKDIR /home/synereo/synereo
RUN sbt gloseval/universal:packageZipTarball
RUN tar xzvf gloseval/target/universal/gloseval-*.tgz
RUN mkdir -p /opt/synereo/gloseval
RUN mv gloseval-*/* /opt/synereo/gloseval

WORKDIR /home/synereo
RUN rm -rf /home/synereo/synereo
RUN rm -rf /home/synereo/.ivy2

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY eval.conf /home/synereo/eval.conf
COPY log.conf /home/synereo/log.conf

RUN /opt/synereo/gloseval/bin/gloseval gencert --self-signed

EXPOSE 8567 9876

USER root

CMD ["/usr/bin/supervisord"]
