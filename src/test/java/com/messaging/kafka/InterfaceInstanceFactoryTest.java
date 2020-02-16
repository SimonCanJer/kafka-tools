package com.messaging.kafka;


import com.messaging.kafka.admin.api.IAdminSite;
import org.junit.Test;

import static org.junit.Assert.*;

public class InterfaceInstanceFactoryTest {


    @Test
    public void factory() {
        Throwable error=null;
        try {
            IAdminSite site = IAdminSite.factory.get();
            assertNotNull(site);
        }
        catch(Throwable r)
        {
            error= r;
        }
        assertNull(error);

    }
}