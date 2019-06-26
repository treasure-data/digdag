IMAGE_NAME = digdag/digdag-build
IMAGE_TAG = $(shell git show --pretty=format:'%ad-%H' --date 'format:%Y%m%dT%H%M%S' | head -n 1)
IMAGE = $(IMAGE_NAME):$(IMAGE_TAG)

build:
	cp ../digdag-docs/requirements.txt bootstrap/
	cp ../digdag-docs/constraints.txt bootstrap/
	docker build -t $(IMAGE) .
	@echo Built $(IMAGE)

push:
	docker push $(IMAGE)
