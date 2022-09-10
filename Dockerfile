FROM python:3.8
COPY requirements.txt /preprocesamiento/requirements.txt
WORKDIR /preprocesamiento
RUN pip install -r requirements.txt
COPY . /preprocesamiento
VOLUME /tmp
ENTRYPOINT ["python"]
CMD ["main.py"]