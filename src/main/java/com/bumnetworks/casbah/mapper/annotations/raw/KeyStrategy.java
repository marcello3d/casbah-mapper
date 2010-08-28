package com.bumnetworks.casbah.mapper.annotations.raw;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.bumnetworks.casbah.mapper.MapKeyStrategy;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface KeyStrategy {
    Class value();
}
