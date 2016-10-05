FROM synereo/base

WORKDIR /home/synereo

COPY gloseval-*.tgz /home/synereo/gloseval.tgz
RUN tar xzvf gloseval.tgz
RUN mkdir -p /opt/synereo/gloseval
RUN mv gloseval-*/* /opt/synereo/gloseval

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY eval.conf /home/synereo/eval.conf
COPY log.conf /home/synereo/log.conf

RUN /opt/synereo/gloseval/bin/gloseval gencert --self-signed

EXPOSE 8567 9876

USER root

CMD ["/usr/bin/supervisord"]
