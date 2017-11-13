package umb.guice;

import com.google.inject.AbstractModule;
import umb.common.UmbSocketChannel;
import umb.common.UmbSocketChannelImpl;

public class UmbModule extends AbstractModule{
    @Override
    protected void configure() {
        bind(UmbSocketChannel.class).to(UmbSocketChannelImpl.class);
    }
}
