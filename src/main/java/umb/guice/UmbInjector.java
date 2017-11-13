package umb.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class UmbInjector {
    public static Injector injector;
    static {
        injector = Guice.createInjector(new UmbModule());
    }
}
