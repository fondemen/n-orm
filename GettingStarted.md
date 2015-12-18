# Requirements #

You need to have installed:
  * [Java](http://java.sun.com) JDK v.1.6.0
  * [mercurial](http://mercurial.selenic.com) in case you want sources
  * [maven 3](http://maven.apache.org/) for building the project and work with it
  * [the AspectJ compiler](http://www.eclipse.org/aspectj) in case you don't want to work with maven

# Sample Project #

A standalone configured project is available for [download](http://code.google.com/p/n-orm/downloads/detail?name=sample-project.zip) and [in the repository](http://code.google.com/p/n-orm/source/checkout).

Once you have it, just unzip, cd into the main folder and run `mvn test` to see it at work. The first time, it may take a while as many different libraries are to be downloaded.

Otherwise, you can open it as an [Eclipse](http://www.eclipse.org/) project with the [m2eclipse](http://m2eclipse.sonatype.org/) and [AspectJ](http://www.eclipse.org/ajdt/) plugins enabled. In case you miss those plugins, you may find them in the Eclipse market: use menu Help / Eclipse marketplace ; you can search for "maven integration for eclipse" to get m2eclipse installed, and "AspectJ" to get the AJDT installed. Now, import the sample project with menu File / Import / General / Existing project. For obscure reasons, once imported, you may need to clean the project by invoking menu Project/clean. Once the project is OK, you can run the tests in src/test/java by right-clicking on the latter folder, and invoking menu Run as / JUnit test.

# Compiling from sources #

This part is optional.

Once mercurial is installed, you can get the sources by executing the following command:<br>
<pre><code>hg clone https://n-orm.googlecode.com/hg/ n-orm<br>
</code></pre>

You can then enter the created n-orm folder:<br>
<pre><code>cd n-orm<br>
</code></pre>

You have there the latest (development) version for n-orm.<br>
In case you prefer working with a more stable version, or in case you want to select another version for your data store, use another branch. At the time of writing, other branches are <code>hbase-0.90.3</code>, <code>hbse-0.90.2</code>, and <code>hbase-0.90.0</code>. Please note that the <code>hbase 0.20.6</code> isn't supported anymore.<br>
To change branch, you can now invoke<br>
<pre><code>hg up &lt;branch&gt;<br>
</code></pre>

Once you have downloaded the sources, the project can be compiled using maven:<br>
<pre><code>mvn install -P hbase<br>
</code></pre>

In case you prefer working with Cloudera's CDH, invoke install using the hbase-cdh profile:<br>
<pre><code>mvn install -P hbase-cdh<br>
</code></pre>

This operation may take a long time the first time as numerous libraries need to be downloaded from maven central.<br>
<br>
If the compilation process is OK and if tests pass, n-orm will be available in your local maven repository. Jars are available in <code>storage/target</code> and <code>hbase/target</code> folders.<br>
<br>
<h2>Updating project</h2>

Sources for new releases of the project may be obtained by merely invoking <code>hg pull -u</code> in the n-orm folder.<br>
<br>
Once updated, you need to compile again.<br>
<pre><code>mvn clean install -P hbase<br>
</code></pre>

<h1>Using n-orm</h1>

Usage of maven is highly recommended (see next chapter). If you prefer to use <a href='http://ant.apache.org/'>ant</a>, give a try to <a href='http://ant.apache.org/ivy/'>ivy</a>, and edit this page to share your experience ;-). However, if you don't want to use maven, here is some more information you need to be aware of.<br>
<br>
n-orm makes use of <a href='http://en.wikipedia.org/wiki/Aspect-oriented_programming'>aspect-oriented programming</a>, and more specifically of <a href='http://www.eclipse.org/aspectj'>AspectJ</a>, which requires your project to obey some particular rules.<br>
<br>
Aspect-oriented programming is a mean to integrate new behavior into existing code. One may see it as some (very special) kind of patch mechanism that works according to the semantics of the language (Java in our case) instead of depending on line numbers (even though this view is very raw and restrictive).<br>
<br>
In order to work as expected, your data model, made of annotated Java classes, needs to be "exposed" to the n-orm aspects. To do so, the easier is to use the <a href='http://www.eclipse.org/aspectj/doc/released/devguide/ajc-ref.html'>ajc tool</a> instead of the javac tool to compile your data classes. Most javac options are available in the ajc tool so that you do not need to change the way you work too much. Just set the <code>-aspectpath</code> option to the <a href='https://oss.sonatype.org/content/groups/public/com/googlecode/n_orm/storage/0.0.1-SNAPSHOT/'>n-orm jar</a> (e.g. -aspectpath lib/storage-0.0.1-20110930.163102-87.jar) not forgetting dependencies (see <a href='http://code.google.com/p/n-orm/source/browse/storage/pom.xml'>storage</a> and <a href='http://code.google.com/p/n-orm/source/browse/parent-aspect/pom.xml'>parent-aspect</a> POMs for all dependencies). Note that the n-orm jars also have all to to be in your classpath both at compile-time and runtime, together with dependencies.<br>
<br>
To simplify jar management a bit, you can create a jar with full n-orm capabilities and including its dependencies by compiling it using the following command:<br>
<pre><code>cd n-orm/hbase<br>
mvn assembly:single<br>
</code></pre>
Jar is then available under <code>hbase/target/hbase-hbase-&lt;HBASE-version&gt;-n-orm-&lt;N-ORM-version&gt;-jar-with-dependencies.jar</code>. This jar can be used for compiling (using the aspectpath option) and running (as part of the classpath) time.<br>
<br>
<h1>Using n-orm with maven</h1>

No project archetype is developed so far. The easiest is to inspire yourself from the <a href='http://code.google.com/p/n-orm/source/browse/sample/pom.xml'>POM for the sample project</a>.<br>
<br>
To use n-orm in your maven project, just add the following lines in your pom.xml file:<br>
<pre><code>        &lt;dependencies&gt;<br>
                &lt;dependency&gt;<br>
                        &lt;groupId&gt;com.googlecode.n_orm&lt;/groupId&gt;<br>
                        &lt;artifactId&gt;storage&lt;/artifactId&gt;<br>
                        &lt;version&gt;${n-orm.version}&lt;/version&gt;<br>
                        &lt;type&gt;jar&lt;/type&gt;<br>
                        &lt;scope&gt;compile&lt;/scope&gt;<br>
                &lt;/dependency&gt;<br>
        &lt;/dependencies&gt;<br>
        &lt;repositories&gt;<br>
                &lt;repository&gt;<br>
                        &lt;id&gt;org.sonatype.oss.public&lt;/id&gt;<br>
                        &lt;name&gt;OSS public&lt;/name&gt;<br>
                        &lt;url&gt;http://oss.sonatype.org/content/groups/public&lt;/url&gt;<br>
                &lt;/repository&gt;<br>
        &lt;/repositories&gt;<br>
</code></pre>
Latest n-orm version is visible in <a href='http://code.google.com/p/n-orm/source/browse/pom.xml'>the parent pom</a>. Either add a property to your POM, or replace <code>${n-orm.version}</code> by the version you want. An example is <code>0.0.1-SNAPSHOT</code>.<br>
<br>
The <code>repository</code> part makes you avoid downloading sources and compiling.<br>
<br>
In addition, you need <a href='http://www.eclipse.org/aspectj'>AspectJ</a> to be enabled in the project where resides the data model. You thus need to use the <a href='http://mojo.codehaus.org/aspectj-maven-plugin/'>AspectJ plugin for maven</a> and weave your project with the aspects defined in the <a href='http://code.google.com/p/n-orm/source/browse/storage'>storage</a> project. Again, improve your pom.xml by adding:<br>
<pre><code>	&lt;build&gt;<br>
		&lt;plugins&gt;<br>
			&lt;plugin&gt;<br>
				&lt;groupId&gt;org.codehaus.mojo&lt;/groupId&gt;<br>
				&lt;artifactId&gt;aspectj-maven-plugin&lt;/artifactId&gt;<br>
				&lt;version&gt;1.4&lt;/version&gt;<br>
				&lt;configuration&gt;<br>
					&lt;source&gt;1.6&lt;/source&gt;<br>
					&lt;target&gt;1.6&lt;/target&gt;<br>
					&lt;complianceLevel&gt;1.6&lt;/complianceLevel&gt;<br>
					&lt;aspectLibraries&gt;<br>
						&lt;aspectLibrary&gt;<br>
							&lt;groupId&gt;com.googlecode.n_orm&lt;/groupId&gt;<br>
							&lt;artifactId&gt;storage&lt;/artifactId&gt;<br>
						&lt;/aspectLibrary&gt;<br>
					&lt;/aspectLibraries&gt;<br>
				&lt;/configuration&gt;<br>
				&lt;executions&gt;<br>
					&lt;execution&gt;<br>
						&lt;goals&gt;<br>
							&lt;goal&gt;compile&lt;/goal&gt;       &lt;!-- use this goal to weave all your main classes --&gt;<br>
							&lt;goal&gt;test-compile&lt;/goal&gt;  &lt;!-- use this goal to weave all your test classes --&gt;<br>
						&lt;/goals&gt;<br>
					&lt;/execution&gt;<br>
				&lt;/executions&gt;<br>
			&lt;/plugin&gt;<br>
		&lt;/plugins&gt;<br>
	&lt;/build&gt;<br>
<br>
</code></pre>

To <a href='WorkingWithHBase.md'>work with HBase</a>, you need to add one more dependency:<br>
<pre><code>                &lt;dependency&gt;<br>
                        &lt;groupId&gt;com.googlecode.n_orm&lt;/groupId&gt;<br>
                        &lt;artifactId&gt;hbase&lt;/artifactId&gt;<br>
                        &lt;version&gt;hbase-${hbase.version}-n_orm-${n-orm.version}&lt;/version&gt;<br>
                        &lt;type&gt;jar&lt;/type&gt;<br>
                        &lt;scope&gt;compile&lt;/scope&gt;<br>
                &lt;/dependency&gt;<br>
</code></pre>

<h1>First data model</h1>

The easiest way to learn how to write a data model is to read the tests:<br>
<ul><li>A data model is given in <a href='http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/BookStore.java'>BookStore</a> <a href='http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/Book.java'>Book</a> and <a href='http://code.google.com/p/n-orm/source/browse/sample/src/main/java/com/googlecode/n_orm/sample/businessmodel/Novel.java'>Novel</a>
</li></ul><ul><li>Sample usages given in the <a href='http://code.google.com/p/n-orm/source/browse/sample/src/test/java/com/googlecode/n_orm/sample/businessmodel/BasicTest.java'>BasicTest</a> test</li></ul>

The <a href='DataModel.md'>DataModel</a> article may also be helpful.<br>
<br>
<h1>Selecting a store</h1>

Once your data model is created, you need to state in which <a href='http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/storeapi/Store.html'>store</a> your persisting elements are to be stored and retrieved from. This is merely done by placing a <code>store.properties</code> file in the CLASSPATH. Stores are defined at package level, and should be placed in the package of your data model. For instance, the store for class <code>com.mycompany.myproject.mycomponent.MyClass</code>will be searched in <code>com/mycompany/myproject/mycomponent/store.properties</code> anywhere in the CLASSPATH. If this resource is not found, it will the be searched in <code>com/mycompany/myproject/store.properties</code>, <code>com/mycompany/store.properties</code>, <code>com/store.properties</code> and <code>store.properties</code>.<br>
<br>
Available stores are the following:<br>
<table><thead><th> <b>Store</b> </th><th> <b>Description</b> </th><th> <b><code>store.properties</code></b> </th></thead><tbody>
<tr><td> <a href='http://wiki.n-orm.googlecode.com/hg/storage/apidocs/com/googlecode/n_orm/memory/Memory.html'>Memory</a> </td><td> Simple in-memory store ; nice for testing. Not serialized, i.e. looses all its data as soon as the JVM stops. </td><td> <code>class=com.googlecode.n_orm.memory.Memory</code><br><code>singleton=INSTANCE</code> </td></tr>
<tr><td> <a href='http://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html'>HBase</a> - <a href='http://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html'>alternative</a> - see <a href='WorkingWithHBase.md'>WorkingWithHBase</a></td><td> HBase store. Version available in the maven repositories are for hbase 0.90.3, 0.90.2, 0.90.0 and cloudera's 0.90.3-cdh3u1, 0.90.1-cdh3u0. If you need another client version, change property <code>hbase.version</code> in <a href='http://code.google.com/p/n-orm/source/browse/hbase-test-deps/pom.xml'>hbase-test-deps</a> and <a href='http://code.google.com/p/n-orm/source/browse/hbase/pom.xml'>hbase</a> POMs, and compile as described above. In some case, that work is already done in a branch of the project. </td><td> <code>class=</code><a href='http://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html'>com.googlecode.n_orm.hbase.Store</a> or <a href='http://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/HBase.html'>com.googlecode.n_orm.hbase.HBase</a><br><code>static-accessor=getStore</code><br><code>1=</code>comma-separated HBase and Hadoop locations (e.g. <code>/usr/lib/hbase,/usr/lib/hadoop</code>)<br> <code>compression=</code>none, gz, lzo, or snappy (if installed on you cluster) ; if omitted, it is assumed 'none' ; see <a href='http://hbase.apache.org/book/perf.configurations.html#perf.compression'>HBase documentation</a><br> <code>forceCompression=</code>true or false ; makes possible for existing tables to be changed their compressor in case it's not that one defined by the compression parameter<br><code>scanCaching=</code> the number of elements downloaded from the base while searching ; default value is 1 ; see <a href='http://hbase.apache.org/book/perf.reading.html#perf.hbase.client.caching'>HBase documentation</a><br> see the <a href='http://wiki.n-orm.googlecode.com/hg/hbase/apidocs/com/googlecode/n_orm/hbase/Store.html'>javadoc</a> for more options (see setter methods ; also works with the com.googlecode.n_orm.hbase.HBase launcher) </td></tr>
<tr><td> <a href='http://wiki.n-orm.googlecode.com/hg/redis/apidocs/com/googlecode/n_orm/redis/RedisStore.html'>Redis</a> </td><td> Redis store. Based upon <a href='https://github.com/xetorthio/jedis'>Jedis</a> 2.0.0</td><td> <code>class=com.googlecode.n_orm.redis.RedisStore</code><br><code>static-accessor=getStore</code><br><code>1=</code>server host (e.g. localhost)<br><code>2=</code> server port (e.g.  6379)</td></tr>