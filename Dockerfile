FROM public.ecr.aws/lambda/python:3.8

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . ${LAMBDA_TASK_ROOT}

CMD [ "main.handler" ]

