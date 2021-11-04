FROM ubuntu:20.04
ENV TZ=Europe/Paris
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get -y update
RUN rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash phil && echo "phil:phil" | chpasswd && adduser phil sudo

WORKDIR /home/phil/

USER phil

ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]
