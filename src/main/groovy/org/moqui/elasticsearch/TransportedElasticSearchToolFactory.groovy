/*
 * This software is in the public domain under CC0 1.0 Universal plus a 
 * Grant of Patent License.
 * 
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.moqui.elasticsearch

import groovy.transform.CompileStatic
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ToolFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.moqui.impl.context.ExecutionContextFactoryImpl
/** ElasticSearch Client is used for indexing and searching documents */    
/** NOTE: embedded ElasticSearch may soon go away, see: https://www.elastic.co/blog/elasticsearch-the-server */
@CompileStatic
class TransportedElasticSearchToolFactory implements ToolFactory<Client> {
    protected final static Logger logger = LoggerFactory.getLogger(TransportedElasticSearchToolFactory.class)
    final static String TOOL_NAME = "ElasticSearch"

    protected ExecutionContextFactory ecf = null
    /** ElasticSearch Node */
    protected org.elasticsearch.node.Node elasticSearchNode
    /** ElasticSearch Client */
    protected Client elasticSearchClient

    /** Default empty constructor */
    TransportedElasticSearchToolFactory() { }

    @Override
    String getName() { return TOOL_NAME }
    @Override
    void init(ExecutionContextFactory ecf)  {
        this.ecf = ecf
        // set the ElasticSearch home (for config, modules, plugins, scripts, etc), data, and logs directories
        // see https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-dir-layout.html
        // NOTE: could use getPath() instead of toExternalForm().substring(5) for file specific URLs, will work on Windows?
        String pathHome = ecf.resource.getLocationReference("component://moqui-elasticsearch/home").getUrl().toExternalForm().substring(5)
        String pathData = ecf.runtimePath + "/elasticsearch/data"
        String pathLogs = ecf.runtimePath + "/log"
        logger.info("Starting ElasticSearch, home at ${pathHome}, data at ${pathData}, logs at ${pathLogs}")

        // some code to cleanup the classpath, avoid jar hell IllegalStateException
        String initialClassPath = System.getProperty("java.class.path")
        StringBuilder newClassPathSb = new StringBuilder()
        String pathSeparator = System.getProperty("path.separator")
        Set<String> cpEntrySet = new HashSet<>()
        if (initialClassPath) for (String cpEntry in initialClassPath.split(pathSeparator)) {
            if (!cpEntry) {
                logger.warn("Found empty classpath entry, removing as ElasticSearch jar hell will blow up")
                continue
            }
            if (cpEntrySet.contains(cpEntry)) {
                logger.warn("Found duplicate classpath entry ${cpEntry}, removing as ElasticSearch jar hell will blow up")
                continue
            }
            cpEntrySet.add(cpEntry)
            if (newClassPathSb.length() > 0) newClassPathSb.append(pathSeparator)
            newClassPathSb.append(cpEntry)
        }
        System.setProperty("java.class.path", newClassPathSb.toString())
        // logger.info("Before ElasticSearch java.class.path: ${System.getProperty('java.class.path')}")

        // build the ES node
        Settings.Builder settings = Settings.builder()
        ExecutionContextFactoryImpl ecfi = (ExecutionContextFactoryImpl)ecf;
        def toolsNode = ecfi.getConfXmlRoot().first("tools")
        String server = toolsNode.attribute("elastic-server");
        String port = toolsNode.attribute("elastic-port");
        if(!server || !port) {
            logger.error("Could not find elastic-server/elastic-port attribute on the tools element of the conf. To use the embedded version please enable the ElasticSearchToolFactory and disable TransportedElasticSearchToolFactory in MoquiConf.xml")
        }
        logger.info("Starting elastic search transport client running against ${server}:${port}")
        settings.put("cluster.name", "MoquiElasticSearch")    

        Client client = new PreBuiltTransportClient(settings.build()).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(server), port.toInteger()));
        elasticSearchClient = client
    }

    @Override
    void preFacadeInit(ExecutionContextFactory ecf) { }

    @Override
    Client getInstance(Object... parameters) {
        if (elasticSearchClient == null) throw new IllegalStateException("ElasticSearchToolFactory not initialized")
        return elasticSearchClient
    }

    @Override
    void destroy() {
        if (elasticSearchNode != null) try {
            elasticSearchNode.close()
            while (!elasticSearchNode.isClosed()) {
                logger.info("ElasticSearch still closing")
                this.wait(1000)
            }
            logger.info("ElasticSearch closed")
        } catch (Throwable t) { logger.error("Error in ElasticSearch node close", t) }
    }

    ExecutionContextFactory getEcf() { return ecf }
}
