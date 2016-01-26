#!/bin/sh

java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase batch conf/batch-lg.yaml | grep BatchAnalyzer | tee lg.out
java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" macrobase.MacroBase batch conf/batch-sm.yaml | grep BatchAnalyzer | tee sm.out
