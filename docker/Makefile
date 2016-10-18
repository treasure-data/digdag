GIT_SHOW = $(shell git log -n 1 --pretty=format:%H)
TS = $(shell date -u +%Y%m%dT%H%M%S)
GIT_STATUS = $(shell git status --porcelain --ignore-submodules=dirty 2> /dev/null | tail -n 1)

ifeq ($(strip $(GIT_STATUS)),)
	IMAGE_TAG = $(TS)-$(GIT_SHOW)
else
	IMAGE_TAG = $(TS)-$(GIT_SHOW)-dirty
endif

IMAGE_NAMESPACE = digdag
IMAGE_NAME = digdag-build
IMAGE = $(IMAGE_NAMESPACE)/$(IMAGE_NAME):$(IMAGE_TAG)

build:
	docker build -t $(IMAGE) .

push:
	docker push $(IMAGE)
