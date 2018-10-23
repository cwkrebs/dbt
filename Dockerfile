FROM python:3.6

COPY requirements.txt ./
RUN apt-get update \
		&& apt-get install -y libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit \
		&& pip install pip --upgrade \
		&& pip install --no-cache-dir -r requirements.txt \
        && apt-get clean && mkdir -p /root/.dbt

RUN pip install virtualenv
RUN pip install virtualenvwrapper
RUN pip install tox

COPY ./profiles.yml /root/.dbt/

EXPOSE 8080

WORKDIR /usr/src/app
RUN cd /usr/src/app
