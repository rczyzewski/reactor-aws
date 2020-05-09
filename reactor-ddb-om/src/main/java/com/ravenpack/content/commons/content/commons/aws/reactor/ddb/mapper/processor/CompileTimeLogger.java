package com.ravenpack.content.commons.content.commons.aws.reactor.ddb.mapper.processor;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.WARNING;

@AllArgsConstructor
public class CompileTimeLogger implements Logger
{
    private Messager msg;

    @SneakyThrows
    private void msg(Diagnostic.Kind kind, String arg)
    {
        TimeUnit.MILLISECONDS.sleep(1);
        String date = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        msg.printMessage(kind, format("%s %s", date, arg));
    }

    public void info(String arg)
    {
        msg(WARNING, arg);
    }

    public void warn(String arg)
    {
        msg(WARNING, arg);
    }

    public void error(String arg)
    {
        msg(ERROR, arg);
    }
}
