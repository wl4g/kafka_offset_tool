rem
rem Copyright 2017 ~ 2025 the original author or authors[983708408@qq.com].
rem
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy of the License at
rem
rem      http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
setlocal enabledelayedexpansion
rem Using pushd popd to set BASE_DIR to the absolute path
pushd %~dp0..\..
set BASE_DIR=%CD%
popd
mkdir -p %BASE_DIR%\..\bin
SET CGO_ENABLED=0
SET GOOS=windows
SET GOARCH=amd64
SET BUILD_BRANCH=git branch|findstr "*"
SET BUILD_VERSION=%BUILD_BRANCH:~2,20%
go build -v -a -ldflags '-s -w' -gcflags="all=-trimpath=%BASE_DIR%" -asmflags="all=-trimpath=%BASE_DIR%" -o %BASE_DIR%\..\bin\kafkaOffsetTool_%BUILD_VERSION%_%GOOS%_%GOARCH% %BASE_DIR%\..\pkg\