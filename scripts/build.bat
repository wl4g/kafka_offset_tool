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

rem -------------------------------------------------------------------------
rem --- Compiling Mac and Linux 64-bit executable programs under Windows. ---
rem -------------------------------------------------------------------------
SET CGO_ENABLED=0
SET GOOS=darwin
SET GOOS=linux
SET GOARCH=amd64
go build -o kafkaOffsetTool ..\pkg\