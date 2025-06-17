FROM ubuntu:22.04
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip install flask==3.0.*
RUN pip install notebook
RUN useradd -ms /bin/bash barch
WORKDIR /home/barch
USER barch
RUN mkdir -p /home/barch/setup/test
COPY Release/_barch.so /home/barch/setup/_barch.so
COPY Release/barch.py /home/barch/setup/barch.py
COPY Release/setup.py /home/barch/setup/setup.py
COPY ./test/*.py /home/barch/setup/test/
COPY ./docker/valkey/valkey.conf ./valkey.conf
COPY ./docker/valkey/valkey.conf ./setup/valkey.conf
RUN pip install ./setup
RUN pip install IPython
COPY ./test/Release/_deps/valkey-src/src/valkey-server /home/barch/setup/valkey-server
COPY ./test/Release/_deps/valkey-src/src/valkey-cli /home/barch/setup/valkey-cli
COPY ./test/Release/_deps/valkey-src/src/valkey-benchmark /home/barch/setup/valkey-benchmark

COPY ./examples/flask/example.py ./example.py

ENV FLASK_APP=example
EXPOSE 8000
CMD ["flask", "run", "--host", "0.0.0.0", "--port", "8000"]

