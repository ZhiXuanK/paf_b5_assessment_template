package vttp.batch5.paf.movies;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import vttp.batch5.paf.movies.bootstrap.Dataloader;
import vttp.batch5.paf.movies.exception.DataException;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@SpringBootApplication
public class MoviesApplication implements CommandLineRunner{

	@Value("${movies.zipfile}")
  	private String moviesZip;

	@Autowired
	private MySQLMovieRepository sqlRepo;

	@Autowired
	private Dataloader dataloader;

	public static void main(String[] args) {
		SpringApplication.run(MoviesApplication.class, args);
	}

	@Override
	public void run(String... args) throws IOException, DataException{
		if (sqlRepo.checkSize() < 1){
			dataloader.unzipFile(moviesZip);
		}
	}

}
