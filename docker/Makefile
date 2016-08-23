GIT_SHOW = $(shell git show --pretty=format:'%H-%ad' --date 'format:%Y%m%dT%H%M%S' | head -n 1)
GIT_STATUS = $(shell git status --porcelain --ignore-submodules=dirty 2> /dev/null | tail -n1)

ifeq ($(strip $(GIT_STATUS)),)
	IMAGE_TAG = $(GIT_SHOW)
else
	IMAGE_TAG = $(GIT_SHOW)-dirty
endif

IMAGE_NAMESPACE = digdag
IMAGE_NAME = digdag-build
IMAGE = $(IMAGE_NAMESPACE)/$(IMAGE_NAME):$(IMAGE_TAG)

build:
	docker build -t $(IMAGE) .

push:
	docker push $(IMAGE)
