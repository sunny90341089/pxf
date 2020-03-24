package org.greenplum.pxf.api.model;

import org.apache.hadoop.conf.Configuration;

/**
 * Base class for all {@link DataSplitter} types. Initializes the
 * {@link Plugin} with the given {@link RequestContext} and
 * {@link Configuration}
 */
public abstract class BaseDataSplitter
    extends BasePlugin implements DataSplitter {

    /**
     * Constructs a {@link DataSplitter} and initializes the {@link Plugin}
     *
     * @param context       the request context for the given query
     * @param configuration the configuration for the server we are accessing
     */
    public BaseDataSplitter(RequestContext context,
                            Configuration configuration) {
        initialize(context, configuration);
    }
}