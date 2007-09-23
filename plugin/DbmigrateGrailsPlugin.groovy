
class DbmigrateGrailsPlugin {
    def title = "Database migration tasks"
    def author = "Sam Pullara"
    def authorEmail = "sam@sampullara.com"
    def description = '''\
The dbmigrate plugin will allow you to track changes to your database over time and ensure that your database
is always up-to-date with the code that is using it.
'''
    def documentation = "http://code.google.com/p/dbmigrate/wiki/Grails"
    def version = 0.1
	def dependsOn = [:]
	
	def doWithSpring = {
		// TODO Implement runtime spring config (optional)
	}   
	def doWithApplicationContext = { applicationContext ->
		// TODO Implement post initialization spring config (optional)		
	}
	def doWithWebDescriptor = { xml ->
		// TODO Implement additions to web.xml (optional)
	}	                                      
	def doWithDynamicMethods = { ctx ->
		// TODO Implement additions to web.xml (optional)
	}	
	def onChange = { event ->
		// TODO Implement code that is executed when this class plugin class is changed  
		// the event contains: event.application and event.applicationContext objects
	}                                                                                  
	def onApplicationChange = { event ->
		// TODO Implement code that is executed when any class in a GrailsApplication changes
		// the event contain: event.source, event.application and event.applicationContext objects
	}
}
