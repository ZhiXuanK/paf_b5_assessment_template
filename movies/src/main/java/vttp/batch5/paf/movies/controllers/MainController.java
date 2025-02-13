package vttp.batch5.paf.movies.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.json.JsonArray;
import vttp.batch5.paf.movies.services.MovieService;

@RestController
@RequestMapping("/api")
public class MainController {

  @Autowired
  private MovieService movieSvc;

  // TODO: Task 3
  @GetMapping(path = { "/summary" }, produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<String> getProlificDirectors(
      @RequestParam String count
  ) {

    JsonArray response = movieSvc.getProlificDirectors(Integer.parseInt(count));

    return ResponseEntity.ok().body(response.toString());

  }

  // TODO: Task 4

}
