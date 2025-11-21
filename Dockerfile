FROM ubuntu:24.04
RUN apt-get update -y && apt-get install -y python3 python3-pip

RUN pip install flask==3.0.*
RUN pip install notebook
RUN useradd -ms /bin/bash barch
WORKDIR /home/barch
USER barch
RUN mkdir -p /home/barch/setup/test
COPY build/_barch.so /home/barch/setup/_barch.so
COPY build/barch.py /home/barch/setup/barch.py
COPY build/setup.py /home/barch/setup/setup.py
COPY ./test/*.py /home/barch/setup/test/
COPY ./docker/valkey/valkey.conf ./valkey.conf
COPY ./docker/valkey/valkey.conf ./setup/valkey.conf
RUN pip install ./setup
RUN pip install IPython
COPY ./test/build/_deps/valkey-src/src/valkey-server /home/barch/setup/valkey-server
COPY ./test/build/_deps/valkey-src/src/valkey-cli /home/barch/setup/valkey-cli
COPY ./test/build/_deps/valkey-src/src/valkey-benchmark /home/barch/setup/valkey-benchmark
COPY ./examples/flask/example.py ./example.py
COPY ./docker/start.sh ./start.sh
USER root
RUN chmod +x start.sh
USER barch
ENV FLASK_APP=example
EXPOSE 8000
EXPOSE 6379
EXPOSE 14000
CMD ["./start.sh"]
