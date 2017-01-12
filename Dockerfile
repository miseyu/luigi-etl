FROM gcr.io/oz-analytics-01/python-gcloud-base:1.0.0

RUN apt-get update && apt-get install -y curl

RUN echo "==> Download & install..."  && \
curl -o install.sh -L https://td-toolbelt.herokuapp.com/sh/install-debian-jessie-td-agent2.sh  && \
 chmod a+x install.sh  && \
 sed -i 's/^sudo -k/#sudo -k/' install.sh  && \
 sed -i 's/^sudo sh/sh/'       install.sh  && \
 ./install.sh  && \
 rm install.sh

ADD . /root
WORKDIR /root
RUN pip install -r requirements.txt
