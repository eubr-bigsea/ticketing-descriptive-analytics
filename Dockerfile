FROM lezzidan/compss-mesos:2.2v4

RUN apt-get update && \
    apt-get install -y unzip

RUN pip install --upgrade pip && \
    pip install numpy pandas netCDF4

ADD https://github.com/eubr-bigsea/ticketing-descriptive-analytics/archive/master.zip /root/

RUN cd /root/ && \
	unzip master.zip
