package com.ravenpack.aws.reactor.ddb;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.time.ZoneOffset.UTC;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LocalDateTimeConverter
{
    public static final String RP_PATTERN = "yyyy-MM-dd' 'HH:mm:ss.SSS";
    public static final DateTimeFormatter RAVENPACK_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern(RP_PATTERN).withZone(UTC);

    public static String toValue(LocalDateTime ldt)
    {
        return RAVENPACK_DATETIME_FORMATTER.format(ldt);
    }

    public static LocalDateTime valueOf(String ldt)
    {

        return LocalDateTime.parse(ldt, RAVENPACK_DATETIME_FORMATTER);
    }
}