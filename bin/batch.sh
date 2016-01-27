#!/bin/sh

stdbuf -i0 -o0 -e0 java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase batch conf/batch-sm.yaml | stdbuf -i0 -o0 -e0 grep BatchAnalyzer | stdbuf -i0 -o0 -e0 tee sm.out
stdbuf -i0 -o0 -e0 java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase batch conf/batch-lg.yaml | stdbuf -i0 -o0 -e0 grep BatchAnalyzer | stdbuf -i0 -o0 -e0 tee lg.out
