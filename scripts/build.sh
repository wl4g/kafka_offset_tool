# Copyright 2017 ~ 2025 the original author or authors[983708408@qq.com].
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASE_DIR=$(cd "`dirname $0`"; pwd)
sudo mkdir -p ${BASE_DIR}/../bin

CGO_ENABLED=0
GOARCH=amd64
GOOS=linux # darwin
go build -v -a -ldflags '-s -w' -gcflags="all=-trimpath=$(pwd)" -asmflags="all=-trimpath=$(pwd)" -o ${BASE_DIR}/../bin/kafkaOffsetTool_${GOOS}_${GOARCH} ${BASE_DIR}/../pkg/
