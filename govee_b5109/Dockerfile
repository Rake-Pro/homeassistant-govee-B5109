ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r /requirements.txt

COPY govee_b5109.py /govee_b5109.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
