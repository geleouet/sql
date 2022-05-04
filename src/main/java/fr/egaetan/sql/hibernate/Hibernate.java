package fr.egaetan.sql.hibernate;

import java.util.Arrays;
import java.util.EnumSet;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.cfg.Configuration;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.schema.TargetType;

public class Hibernate {

	@Entity
	@Table(name = "CLIENT")
	public static class Client {
		@Column
		@Id
		int id;
		
		@Column
		String prenom;
		
		@Column
		int color;
		
		@Column
		int city;
		
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public String getPrenom() {
			return prenom;
		}
		public void setPrenom(String prenom) {
			this.prenom = prenom;
		}
		public int getColor() {
			return color;
		}
		public void setColor(int color) {
			this.color = color;
		}
		public int getCity() {
			return city;
		}
		public void setCity(int city) {
			this.city = city;
		}
		
	}
	
	private void generate(Class dialect, String directory, String... packagesName) throws Exception {

	    MetadataSources metadata = new MetadataSources(
	            new StandardServiceRegistryBuilder()
	                    .applySetting("hibernate.dialect", dialect.getName())
	                    .build());

	    for (String packageName : packagesName) {
	        System.out.println("packageName: " + packageName);
	        for (Class clazz : Arrays.asList(Client.class)) {
	        	System.out.println("Class: " + clazz);
	            metadata.addAnnotatedClass(clazz);
	        }
	    }

	    MetadataImplementor metadataImplementor = (MetadataImplementor) metadata.buildMetadata();
	    SchemaExport export = new SchemaExport();

	    export.setDelimiter(";");
	    String filename = directory + "ddl_" + dialect.getSimpleName().toLowerCase() + ".sql";
	    export.setOutputFile(filename);
	    export.setFormat(true);

	    //can change the output here
	    EnumSet<TargetType> enumSet = EnumSet.of(TargetType.STDOUT);
	    export.execute(enumSet, SchemaExport.Action.CREATE, metadataImplementor);
	}
	
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.setProperty("hibernate.connection.driver_class", "fr.egaetan.sql.driver.SLODriver");
		config.setProperty("hibernate.connection.url", "jdbc:mysql://localhost:3306/bookstoredb");
		//config.setProperty("hibernate.connection.username", "root");
		//config.setProperty("hibernate.connection.password", "password");
		config.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
		
		config.addAnnotatedClass(Client.class);
		
		
		
		 try {
	            SessionFactory sessionFactory = config.buildSessionFactory();  
	            Session session = sessionFactory.openSession();
	            new Hibernate().generate(org.hibernate.dialect.PostgreSQLDialect.class, ".", "");
	            Client user = session.get(Client.class, 1);
	             
	            System.out.println(user.getId());
	            System.out.println(user.getPrenom());
	            System.out.println(user.getColor());
	            
	            user.setColor(255);
	            Transaction transaction = session.beginTransaction();
	            session.save(user);
	            transaction.commit();
	            session.clear();
	            
	            Client user2 = session.get(Client.class, 1);
	            System.out.println(user2.getId());
	            System.out.println(user2.getPrenom());
	            System.out.println(user2.getColor());
	            
	            
	            session.close();
	            sessionFactory.close();
	             
	        } catch (Exception ex) {
	            ex.printStackTrace();
	        }
	}
}
