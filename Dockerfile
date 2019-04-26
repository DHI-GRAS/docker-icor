FROM dhigras/esa-snap:latest

USER root

WORKDIR /app

RUN apt-get update && \
    apt-get -y install \
      'python2.7' 'python-pip'

ADD icor_install_ubuntu_16_04_x64_2.0.0.bin icor-install.bin

RUN yes y | ./icor-install.bin

ENV ICOR_DIR /opt/vito/icor

WORKDIR /work
RUN chmod 777 /work

RUN rm -r /opt/vito/icor/src
ADD icor-src /opt/vito/icor/src

# set entrypoint
ENTRYPOINT ["python2", "/opt/vito/icor/src/icor.py"]
CMD ["--help"]
