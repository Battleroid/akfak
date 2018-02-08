FROM alpine

RUN apk update && apt add python3

COPY ./ /tmp/akfak
RUN cd /tmp/akfak && \
    pip install -r requirements.txt && \
    pip install .

ENTRYPOINT ["akfak"]
