#[macro_use] extern crate lazy_static;

// extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

extern crate threadpool;
extern crate urlencoding;
extern crate regex;
extern crate num_cpus;
extern crate futures;
extern crate tokio_service;
extern crate tokio_proto;
extern crate tokio_minihttp;
extern crate chrono;

use std::{env, fs};
use std::process::Command;
use futures::future;
use tokio_service::Service;
use tokio_proto::TcpServer;
use tokio_minihttp::{Request, Response, Http};
use regex::Regex;
use std::thread;
use std::fs::File;
use std::time::Instant;
use std::collections::HashSet;
use threadpool::ThreadPool;
use std::sync::{Arc, RwLock};
use std::collections::BTreeMap;

use chrono::prelude::*;
use chrono::{DateTime, Utc};

// TODO smart partial updates

lazy_static! {
    static ref USER_FILE_RE: Regex = Regex::new(r"users_\d+.json$").unwrap();
    static ref LOCATION_FILE_RE: Regex = Regex::new(r"locations_\d+.json$").unwrap();
    static ref VISITS_FILE_RE: Regex = Regex::new(r"visits_\d+.json$").unwrap();

    static ref USER_RE: Regex = Regex::new(r"^/users/(?P<id>\d+)[\?]*").unwrap();
    static ref LOCATION_RE: Regex = Regex::new(r"^/locations/(?P<id>\d+)[\?]*").unwrap();
    static ref VISITS_RE: Regex = Regex::new(r"^/visits/(?P<id>\d+)[\?]*").unwrap();

    static ref USER_VISITS_RE: Regex = Regex::new(r"^/users/(?P<id>\d+)/visits[\?]*.*").unwrap();
    static ref LOCATION_MARKS_RE: Regex = Regex::new(r"^/locations/(?P<id>\d+)/avg[\?]*.*").unwrap();

    static ref USER_NEW_RE: Regex = Regex::new(r"^/users/new[\?]*").unwrap();
    static ref LOCATION_NEW_RE: Regex = Regex::new(r"^/locations/new[\?]*").unwrap();
    static ref VISITS_NEW_RE: Regex = Regex::new(r"^/visits/new[\?]*").unwrap();
}

#[derive(Serialize, Deserialize)]
struct Users {
    users: Vec<User>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct User {
    id: i32,
    first_name: String,
    last_name: String,
    gender: String,
    birth_date: i64,
    email: String,
}

#[derive(Debug, Deserialize)]
struct UserPartial {
    first_name: Option<String>,
    last_name: Option<String>,
    gender: Option<String>,
    birth_date: Option<i64>,
    email: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Locations {
    locations: Vec<Location>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Location {
    id: i32,
    distance: i32,
    city: String,
    place: String,
    country: String,
}

#[derive(Deserialize, Debug)]
struct LocationPartial {
    distance: Option<i32>,
    city: Option<String>,
    place: Option<String>,
    country: Option<String>,
}


#[derive(Serialize, Deserialize)]
struct Visits {
    visits: Vec<Visit>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Visit {
    id: i32,
    user: i32,
    location: i32,
    visited_at: i64,
    mark: i8,
}

#[derive(Deserialize, Debug)]
struct VisitPartial {
    user: Option<i32>,
    location: Option<i32>,
    visited_at: Option<i64>,
    mark: Option<i8>,
}

type UserHashMap = Arc<RwLock<BTreeMap<i32, User>>>;
type LocationHashMap = Arc<RwLock<BTreeMap<i32, Location>>>;
type VisitHashMap = Arc<RwLock<BTreeMap<i32, Visit>>>;
type LocationMarkListHashMap = Arc<RwLock<BTreeMap<i32, LocationMarkList>>>;
type UserVisitListHashMap = Arc<RwLock<BTreeMap<i32, UserVisitList>>>;

struct Travels {
    users: UserHashMap,
    locations: LocationHashMap,
    visits: VisitHashMap,
    user_visits: UserVisitListHashMap,
    location_marks: LocationMarkListHashMap,
}

#[derive(Debug, Deserialize)]
struct BodyLength {
    length: usize,
    body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserVisit {
    // search fields
    distance: i32,
    country: String,
    location: i32,
    visit: i32,
    user: i32,

    // packed UserVisitBody
    body: UserVisitBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserVisitBody {
    mark: i8,
    visited_at: i64,
    place: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserVisitList {
    user: i32,
    visits: Vec<UserVisit>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserVisitResponse {
    visits: Vec<UserVisitBody>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LocationMark {
    visited_at: i64,
    // age: i32,
    birth_date: i64,
    // dt: DateTime<Utc>,
    gender: Gender,
    mark: i8,
    user: i32,
    visit: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LocationMarkList {
    location: i32,
    marks: Vec<LocationMark>,
}


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Gender {
    MALE,
    FEMALE,
}

struct SumCount {
    sum: i32,
    count: i32,
}

#[derive(Debug)]
enum QueryField {
    FromDate,
    ToDate,
    FromAge,
    ToAge,
    Country,
    ToDistance,
    Gender,
    Other,
}

#[derive(Debug)]
struct QueryFilter {
    key: QueryField,
    value_i32: Option<i32>,
    value_i64: Option<i64>,
    value_str: Option<String>,
    value_gender: Option<Gender>,
    value_dt: Option<DateTime<Utc>>,
}


fn exists_user(id: i32, items: &UserHashMap) -> bool {
    items.read().ok().and_then(|guard| Some(guard.contains_key(&id))).unwrap_or(false)
}

fn save_user(item: User, items: &UserHashMap) {
    items.write().ok().and_then(|mut guard| { guard.insert(item.id, item) });
}

fn load_user(id: i32, items: &UserHashMap) -> Option<User> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| val.clone()))
}

fn content_user<'a>(id: i32, items: &UserHashMap) -> Option<String> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| serde_json::to_string(val).unwrap_or("".to_string())))
}

fn exists_location(id: i32, items: &LocationHashMap) -> bool {
    items.read().ok().and_then(|guard| Some(guard.contains_key(&id))).unwrap_or(false)
}

fn save_location(item: Location, items: &LocationHashMap) {
    items.write().ok().and_then(|mut guard| { guard.insert(item.id, item) });
}

fn load_location(id: i32, items: &LocationHashMap) -> Option<Location> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| val.clone()))
}

fn content_location<'a>(id: i32, items: &LocationHashMap) -> Option<String> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| serde_json::to_string(val).unwrap_or("".to_string())))
}

fn exists_visit(id: i32, items: &VisitHashMap) -> bool {
    items.read().ok().and_then(|guard| Some(guard.contains_key(&id))).unwrap_or(false)
}

fn save_visit(item: Visit, items: &VisitHashMap) {
    items.write().ok().and_then(|mut guard| { guard.insert(item.id, item) });
}

fn load_visit(id: i32, items: &VisitHashMap) -> Option<Visit> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| val.clone()))
}

fn content_visit<'a>(id: i32, items: &VisitHashMap) -> Option<String> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| serde_json::to_string(val).unwrap_or("".to_string())))
}

fn save_location_mark_list(item: LocationMarkList, items: &LocationMarkListHashMap) {
    items.write().ok().and_then(|mut guard| { guard.insert(item.location, item) });
}

fn load_location_mark_list(id: i32, items: &LocationMarkListHashMap) -> Option<LocationMarkList> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| val.clone()))
}

fn save_user_visits(item: UserVisitList, items: &UserVisitListHashMap) {
    items.write().ok().and_then(|mut guard| { guard.insert(item.user, item) });
}

fn load_user_visits(id: i32, items: &UserVisitListHashMap) -> Option<UserVisitList> {
    items.read().ok().and_then(|guard| guard.get(&id).map(|val| val.clone()))
}

impl Service for Travels {
    type Request = Request;
    type Response = Response;
    type Error = std::io::Error;
    type Future = future::Ok<Response, std::io::Error>;

    fn call(&self, req: Request) -> Self::Future {

        let mut resp = Response::new();

        let mut found = false;
        let path = req.path();
        let is_post = req.method() == "POST";

        if let Some(cap) = USER_RE.captures(path) {
            if is_post {
                let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
                let post_data = req.body();
                if let Ok(user_partial) = serde_json::from_str::<UserPartial>(post_data) {
                    // validate null fields
                    if user_partial.first_name.is_none() {
                        if post_data.contains("\"first_name\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if user_partial.last_name.is_none() {
                        if post_data.contains("\"last_name\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if user_partial.birth_date.is_none() {
                        if post_data.contains("\"birth_date\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if user_partial.email.is_none() {
                        if post_data.contains("\"email\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if user_partial.gender.is_none() {
                        if post_data.contains("\"gender\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }

                    if let Some(mut user) = load_user(id, &self.users) {
                        found = true;
                        if let Some(ref first_name) = user_partial.first_name {
                            user.first_name = first_name.to_string();
                        }
                        if let Some(ref last_name) = user_partial.last_name {
                            user.last_name = last_name.to_string();
                        }
                        if let Some(ref birth_date) = user_partial.birth_date {
                            user.birth_date = *birth_date;
                        }
                        if let Some(ref gender) = user_partial.gender {
                            user.gender = gender.to_string();
                        }
                        if let Some(ref email) = user_partial.email {
                            user.email = email.to_string();
                        }


                        // update user marks
                        if let Some(user_visits) = load_user_visits(user.id, &self.user_visits) {
                            let locations = user_visits.visits.iter().map(|i| i.location).collect::<HashSet<_>>();
                            for location_id in locations.iter() {
                                if let Some(mut location_mark_list) = load_location_mark_list(*location_id, &self.location_marks) {
                                    let mut changed = false;
                                    for item in location_mark_list.marks.iter_mut() {
                                        if item.user == user.id {
                                            changed = true;
                                            item.gender = match user.gender.as_ref() {
                                                "m" => Gender::MALE,
                                                _ => Gender::FEMALE,
                                            };
                                            item.birth_date = user.birth_date;
                                        }
                                    }

                                    if changed {
                                        save_location_mark_list(location_mark_list, &self.location_marks);
                                    }

                                }
                            }
                        }

                        save_user(user, &self.users);
                        resp.body(2, "{}");
                    }
                } else {
                    resp.status_code(400, "Bad Request");
                    return future::ok(resp);
                }
            } else {
                let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
                if let Some(content) = content_user(id, &self.users) {
                    found = true;
                    resp.body(content.len(), content.as_ref());
                }
            }
        }

        if let Some(cap) = LOCATION_RE.captures(&path) {
            if is_post {
                let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
                let post_data = req.body();
                if let Ok(location_partial) = serde_json::from_str::<LocationPartial>(post_data) {
                    if location_partial.distance.is_none() {
                        if post_data.contains("\"distance\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if location_partial.city.is_none() {
                        if post_data.contains("\"city\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if location_partial.place.is_none() {
                        if post_data.contains("\"place\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if location_partial.country.is_none() {
                        if post_data.contains("\"country\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if let Some(mut location) = load_location(id, &self.locations) {
                        found = true;
                        if let Some(distance) = location_partial.distance {
                            location.distance = distance;
                        }
                        if let Some(ref city) = location_partial.city {
                            location.city = city.to_string();
                        }
                        if let Some(ref place) = location_partial.place {
                            location.place = place.to_string();
                        }
                        if let Some(ref country) = location_partial.country {
                            location.country = country.to_string();
                        }

                        // update user visits
                        if let Some(location_mark_list) = load_location_mark_list(location.id, &self.location_marks) {
                            let users = location_mark_list.marks.iter().map(|i| i.user).collect::<HashSet<_>>();
                            for user_id in users.iter() {
                                if let Some(mut user_visits) = load_user_visits(*user_id, &self.user_visits) {
                                    let mut changed = false;
                                    for item in user_visits.visits.iter_mut() {
                                        if item.location == location.id {
                                            changed = true;
                                            item.distance = location.distance;
                                            item.country = location.country.to_owned();
                                            item.body.place = location.place.to_owned();
                                        }
                                    }
                                    if changed {
                                        save_user_visits(user_visits, &self.user_visits);
                                    }
                                };
                            }
                        }

                        save_location(location, &self.locations);

                        resp.body(2, "{}");
                    }
                } else {
                    resp.status_code(400, "Bad Request");
                    return future::ok(resp);
                }
            } else {
                let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
                if let Some(ref content) = content_location(id, &self.locations) {
                    found = true;
                    resp.body(content.len(), content);
                }
            }
        }

        if let Some(cap) = VISITS_RE.captures(&path) {
            if is_post {
                let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
                let post_data = req.body();
                if let Ok(visit_partial) = serde_json::from_str::<VisitPartial>(post_data) {
                    if visit_partial.mark.is_none() {
                        if post_data.contains("\"mark\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if visit_partial.user.is_none() {
                        if post_data.contains("\"user\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if visit_partial.location.is_none() {
                        if post_data.contains("\"location\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if visit_partial.visited_at.is_none() {
                        if post_data.contains("\"visited_at\"") {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }

                    // validate FK
                    if let Some(user) = visit_partial.user {
                        if !exists_user(user, &self.users) {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }
                    if let Some(location) = visit_partial.location {
                        if !exists_location(location, &self.locations) {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                    }

                    if let Some(mut visit) = load_visit(id, &self.visits) {
                        found = true;
                        let mut visited_at_changed = false;
                        let mut old_user: i32 = 0;
                        let mut old_location: i32 = 0;
                        if let Some(mark) = visit_partial.mark {
                            visit.mark = mark;
                        }
                        if let Some(user) = visit_partial.user {
                            if visit.user != user {
                                old_user = visit.user;
                                visit.user = user;
                            }
                        }
                        if let Some(location) = visit_partial.location {
                            if visit.location != location {
                                old_location = visit.location;
                                visit.location = location;
                            }
                        }
                        if let Some(visited_at) = visit_partial.visited_at {
                            visited_at_changed = visit.visited_at != visited_at;
                            visit.visited_at = visited_at;
                        }

                        // if user and location was not changed
                        if old_user == 0 && old_location == 0 {
                            // update user marks
                            if let Some(mut location_mark_list) = load_location_mark_list(visit.location, &self.location_marks) {
                                let mut changed = false;
                                for item in location_mark_list.marks.iter_mut() {
                                    if item.visit == visit.id {
                                        changed = true;
                                        item.visited_at = visit.visited_at;
                                        item.mark = visit.mark;
                                    }
                                }
                                if changed {
                                    save_location_mark_list(location_mark_list, &self.location_marks);
                                }
                            };

                            // update user visits
                            if let Some(mut user_visits) = load_user_visits(visit.user, &self.user_visits) {
                                let mut changed = false;
                                for item in user_visits.visits.iter_mut() {
                                    if item.visit == visit.id {
                                        changed = true;
                                        item.body.visited_at = visit.visited_at;
                                        item.body.mark = visit.mark;
                                    }
                                }
                                if changed {
                                    if visited_at_changed {
                                        user_visits.visits.sort_by(|a, b| a.body.visited_at.cmp(&b.body.visited_at));
                                    }
                                    save_user_visits(user_visits, &self.user_visits);
                                }
                            };
                        }

                        // user was changed
                        if old_user > 0 {
                            // remove old visit from user put it to another
                            if let Some(mut old_user_visits) = load_user_visits(old_user, &self.user_visits) {
                                // find old visit
                                if let Some(index) = old_user_visits.visits.iter().position(|i| i.visit == id) {
                                    // remove old visit
                                    old_user_visits.visits.remove(index);
                                    old_user_visits.visits.sort_by(|a, b| a.body.visited_at.cmp(&b.body.visited_at));
                                    save_user_visits(old_user_visits, &self.user_visits);

                                    if let Some(new_location) = load_location(visit.location, &self.locations) {
                                        // construct body
                                        let user_visit_body = UserVisitBody {
                                            visited_at: visit.visited_at,
                                            mark: visit.mark,
                                            place: new_location.place.to_owned(),
                                        };

                                        let new_visit = UserVisit {
                                            user: visit.user,
                                            visit: visit.id,
                                            location: visit.location,
                                            distance: new_location.distance,
                                            country: new_location.country.to_owned(),
                                            body: user_visit_body,
                                        };

                                        // insert new user visit
                                        if let Some(mut new_user_visits) = load_user_visits(visit.user, &self.user_visits) {
                                            new_user_visits.visits.push(new_visit);
                                            new_user_visits.visits.sort_by(|a, b| a.body.visited_at.cmp(&b.body.visited_at));
                                            save_user_visits(new_user_visits, &self.user_visits);
                                        }
                                    }
                                }
                            }
                        }

                        // location was changed
                        if old_location > 0 {
                            // remove old visit from user put it to another
                            if let Some(mut old_location_marks) = load_location_mark_list(old_location, &self.location_marks) {
                                // find old visit
                                if let Some(index) = old_location_marks.marks.iter().position(|i| i.visit == id) {
                                    // remove old visit
                                    old_location_marks.marks.remove(index);
                                    save_location_mark_list(old_location_marks, &self.location_marks);

                                    // insert new location mark for new location
                                    if let Some(new_user) = load_user(visit.user, &self.users) {
                                        if let Some(mut new_location_marks) = load_location_mark_list(visit.location, &self.location_marks) {
                                            new_location_marks.marks.push(LocationMark {
                                                user: new_user.id,
                                                visit: visit.id,
                                                gender: match new_user.gender.as_ref() {
                                                    "m" => Gender::MALE,
                                                    _ => Gender::FEMALE,
                                                },
                                                birth_date: new_user.birth_date,
                                                mark: visit.mark,
                                                visited_at: visit.visited_at,
                                            });
                                            save_location_mark_list(new_location_marks, &self.location_marks);
                                        }
                                    }
                                }
                            }
                        }

                        save_visit(visit, &self.visits);
                        resp.body(2, "{}");
                    }
                } else {
                    resp.status_code(400, "Bad Request");
                    return future::ok(resp);
                }
            } else {
                let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
                if let Some(content) = content_visit(id, &self.visits) {
                    found = true;
                    resp.body(content.len(), &content);
                }
            }
        }

        if let Some(cap) = USER_VISITS_RE.captures(&path) {
            let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
            if let Some(visits) = load_user_visits(id, &self.user_visits) {
                found = true;

                // parse filters
                if let Some(query_string) = path.split("?").nth(1) {

                    let mut invalid_query_param = false;

                    let params = query_string.split("&").map(|p| {
                        if invalid_query_param {
                            return QueryFilter {
                                key: QueryField::Other,
                                value_i32: None,
                                value_i64: None,
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            };
                        }
                        let mut parts = p.split('=');
                        let key = parts.nth(0).unwrap_or("");

                        match key {
                            "fromDate" => QueryFilter {
                                key: QueryField::FromDate,
                                value_i32: None,
                                value_i64: match parts.nth(0).unwrap_or("").parse::<i64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "toDate" => QueryFilter {
                                key: QueryField::ToDate,
                                value_i32: None,
                                value_i64: match parts.nth(0).unwrap_or("").parse::<i64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "toDistance" => QueryFilter {
                                key: QueryField::ToDistance,
                                value_i32: match parts.nth(0).unwrap_or("").parse::<i32>() {
                                    Ok(value) => Some(value),
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_i64: None,
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "country" => QueryFilter {
                                key: QueryField::Country,
                                value_i32: None,
                                value_i64: None,
                                value_str: match parts.nth(0) {
                                    Some(value) => match urlencoding::decode(value) {
                                        Ok(decoded_value) => Some(decoded_value),
                                        Err(_) => {
                                            invalid_query_param = true;
                                            None
                                        },
                                    },
                                    None => None,
                                },
                                value_gender: None,
                                value_dt: None,
                            },
                            _ => QueryFilter {
                                key: QueryField::Other,
                                value_i32: None,
                                value_i64: None,
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            }
                        }
                    }).collect::<Vec<QueryFilter>>();

                    if invalid_query_param {
                        resp.status_code(400, "Bad Request");
                        return future::ok(resp);
                    }

                    let data = visits.visits.iter().
                        cloned().
                        filter(|v| {
                            for param in &params {
                                match param.key {
                                    QueryField::FromDate => {
                                        if let Some(value_i64) = param.value_i64 {
                                            if v.body.visited_at <= value_i64  {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::ToDate => {
                                        if let Some(value_i64) = param.value_i64 {
                                            if v.body.visited_at >= value_i64 {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::ToDistance => {
                                        if let Some(value_i32) = param.value_i32 {
                                            if v.distance >= value_i32 {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::Country => {
                                        if let Some(ref value_str) = param.value_str {
                                            if &v.country != value_str {
                                                return false;
                                            }
                                        }
                                    },
                                    _ => (),
                                };
                            }
                            true
                        }).
                        map(|v| v.body).collect::<Vec<UserVisitBody>>();

                    let payload = UserVisitResponse {
                        visits: data,
                    };
                    if let Ok(response) = serde_json::to_string(&payload) {
                        resp.body(response.len(), &response);
                    }
                } else {
                    let data = visits.visits.iter().cloned().map(|v| v.body).collect::<Vec<UserVisitBody>>();
                    let payload = UserVisitResponse {
                        visits: data,
                    };
                    if let Ok(response) = serde_json::to_string(&payload) {
                        resp.body(response.len(), &response);
                    }
                }
            }
        }

        if let Some(cap) = LOCATION_MARKS_RE.captures(&path) {
            let id = cap.name("id").map_or("", |m| m.as_str()).parse::<i32>().unwrap_or(-1);
            if let Some(marks) = load_location_mark_list(id, &self.location_marks) {
                found = true;

                // parse filters
                if let Some(query_string) = path.split("?").nth(1) {

                    let mut invalid_query_param = false;

                    let params = query_string.split("&").map(|p| {
                        if invalid_query_param {
                            return QueryFilter {
                                key: QueryField::Other,
                                value_i32: None,
                                value_i64: None,
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            };
                        }
                        let mut parts = p.split('=');
                        let key = parts.nth(0).unwrap_or("");
                        let now = Utc::now();

                        match key {
                            "fromDate" => QueryFilter {
                                key: QueryField::FromDate,
                                value_i32: None,
                                value_i64: match parts.nth(0).unwrap_or("").parse::<i64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "toDate" => QueryFilter {
                                key: QueryField::ToDate,
                                value_i32: None,
                                value_i64: match parts.nth(0).unwrap_or("").parse::<i64>() {
                                    Ok(value) => Some(value),
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "fromAge" => QueryFilter {
                                key: QueryField::FromAge,
                                value_i32: None,
                                value_i64: match parts.nth(0).unwrap_or("").parse::<i32>() {
                                    Ok(value) => if value >= 0 {
                                        if let Some(dt) = now.with_year(now.year() - value) {
                                            Some(dt.timestamp())
                                        } else {
                                            invalid_query_param = true;
                                            None
                                        }
                                    } else {
                                        invalid_query_param = true;
                                        None
                                    },
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "toAge" => QueryFilter {
                                key: QueryField::ToAge,
                                value_i32: None,
                                value_i64: match parts.nth(0).unwrap_or("").parse::<i32>() {
                                    Ok(value) => if value >= 0 {
                                        if let Some(dt) = now.with_year(now.year() - value) {
                                            Some(dt.timestamp())
                                        } else {
                                            invalid_query_param = true;
                                            None
                                        }
                                    } else {
                                        invalid_query_param = true;
                                        None
                                    },
                                    Err(_) => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            },
                            "gender" => QueryFilter {
                                key: QueryField::Gender,
                                value_i32: None,
                                value_i64: None,
                                value_str: None,
                                value_gender: match parts.nth(0).unwrap_or("") {
                                    "m" => Some(Gender::MALE),
                                    "f" => Some(Gender::FEMALE),
                                    _ => {
                                        invalid_query_param = true;
                                        None
                                    }
                                },
                                value_dt: None,
                            },
                            _ => QueryFilter {
                                key: QueryField::Other,
                                value_i32: None,
                                value_i64: None,
                                value_str: None,
                                value_gender: None,
                                value_dt: None,
                            }
                        }
                    }).collect::<Vec<QueryFilter>>();

                    if invalid_query_param {
                        resp.status_code(400, "Bad Request");
                        return future::ok(resp);
                    }

                    let sum = SumCount { sum: 0, count: 0};
                    let result = &marks.marks.iter().
                        filter(|v| {
                            for param in &params {
                                match param.key {
                                    QueryField::FromDate => {
                                        if let Some(value_i64) = param.value_i64 {
                                            if v.visited_at <= value_i64  {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::ToDate => {
                                        if let Some(value_i64) = param.value_i64 {
                                            if v.visited_at >= value_i64 {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::FromAge => {
                                        if let Some(value_i64) = param.value_i64 {
                                            if v.birth_date >= value_i64 {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::ToAge => {
                                        if let Some(value_i64) = param.value_i64 {
                                            if v.birth_date <= value_i64  {
                                                return false;
                                            }
                                        }
                                    },
                                    QueryField::Gender => {
                                        if let Some(ref value_gender) = param.value_gender {
                                            if &v.gender != value_gender {
                                                return false;
                                            }
                                        }
                                    },
                                    _ => (),
                                };
                            }
                            true
                        }).
                        fold(sum, |mut s, val| { s.sum += val.mark as i32; s.count += 1; s });

                    if result.count > 0 {
                        let avg = format!("{:.5}", result.sum as f32 / result.count as f32);
                        let mut avg = avg.trim_right_matches("0").to_string();
                        if avg.ends_with(".") {
                            avg = format!("{}0", avg);
                        }
                        let response = format!("{{\"avg\":{}}}", avg);
                        resp.body(response.len(), &response);
                    } else {
                        let response = "{\"avg\":0}";
                        resp.body(response.len(), &response);
                    }
                } else {
                    let sum = SumCount { sum: 0, count: 0};
                    let result = &marks.marks.iter().fold(sum, |mut s, val| { s.sum += val.mark as i32; s.count += 1; s });
                    if result.count > 0 {
                        let avg = format!("{:.5}", result.sum as f32 / result.count as f32);
                        let mut avg = avg.trim_right_matches("0").to_string();
                        if avg.ends_with(".") {
                            avg = format!("{}0", avg);
                        }
                        let response = format!("{{\"avg\":{}}}", avg);
                        resp.body(response.len(), &response);
                    } else {
                        let response = "{\"avg\":0}";
                        resp.body(response.len(), &response);
                    }
                }
            }
        }

        if is_post && !found {
            let data = req.body();
            if let Some(_) = USER_NEW_RE.captures(path) {
                match serde_json::from_str::<User>(data) {
                    Ok(user) => {
                        found = true;
                        if exists_user(user.id, &self.users) {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                        // initialize user visits
                        let user_visits = UserVisitList {
                            user: user.id,
                            visits: Vec::new(),
                        };
                        save_user_visits(user_visits, &self.user_visits);
                        save_user(user, &self.users);
                        resp.body(2, "{}");
                    },
                    Err(_) => {
                        resp.status_code(400, "Bad Request");
                        return future::ok(resp);
                    }
                };
            }
            if let Some(_) = LOCATION_NEW_RE.captures(path) {
                match serde_json::from_str::<Location>(data) {
                    Ok(location) => {
                        found = true;
                        // println!("New location: {:?}", location);
                        if exists_location(location.id, &self.locations) {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }
                        // initialize location marks
                        let location_mark_list = LocationMarkList {
                            location: location.id,
                            marks: Vec::new(),
                        };
                        save_location_mark_list(location_mark_list, &self.location_marks);
                        save_location(location, &self.locations);
                        resp.body(2, "{}");
                    },
                    Err(_) => {
                        resp.status_code(400, "Bad Request");
                        return future::ok(resp);
                    }
                };
            }
            if let Some(_) = VISITS_NEW_RE.captures(path) {
                match serde_json::from_str::<Visit>(data) {
                    Ok(visit) => {
                        found = true;
                        if exists_visit(visit.id, &self.visits) {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }

                        // process user visits
                        if let Some(mut visits) = load_user_visits(visit.user, &self.user_visits) {
                            if let Some(location) = load_location(visit.location, &self.locations) {
                                // construct body
                                let user_visit_body = UserVisitBody {
                                    visited_at: visit.visited_at,
                                    mark: visit.mark,
                                    place: location.place.to_owned(),
                                };

                                // construct index
                                let user_visit = UserVisit {
                                    user: visit.user,
                                    visit: visit.id,
                                    location: location.id,
                                    distance: location.distance,
                                    country: location.country.to_owned(),
                                    body: user_visit_body,
                                };

                                visits.visits.push(user_visit);
                                visits.visits.sort_by(|a, b| a.body.visited_at.cmp(&b.body.visited_at));

                                save_user_visits(visits, &self.user_visits);

                                // process location marks
                                if let Some(mut marks) = load_location_mark_list(visit.location, &self.location_marks) {
                                    if let Some(user) = load_user(visit.user, &self.users) {
                                        marks.marks.push(LocationMark {
                                            user: user.id,
                                            visit: visit.id,
                                            gender: match user.gender.as_ref() {
                                                "m" => Gender::MALE,
                                                _ => Gender::FEMALE,
                                            },
                                            birth_date: user.birth_date,
                                            // dt: {
                                            //     let ts = NaiveDateTime::from_timestamp(user.birth_date, 0);
                                            //     DateTime::<Utc>::from_utc(ts, Utc)
                                            // },
                                            mark: visit.mark,
                                            visited_at: visit.visited_at,
                                        });

                                        save_location_mark_list(marks, &self.location_marks);
                                    }
                                }

                                save_visit(visit, &self.visits);
                                resp.body(2, "{}");
                            } else {
                                resp.status_code(400, "Bad Request");
                                return future::ok(resp);
                            }
                        } else {
                            resp.status_code(400, "Bad Request");
                            return future::ok(resp);
                        }

                    },
                    Err(_) => {
                        resp.status_code(400, "Bad Request");
                        return future::ok(resp);
                    }
                };
            }
        }

        if !found {
            resp.status_code(404, "Not Found");
        }

        future::ok(resp)
    }
}

fn main() {

    let users = Arc::new(RwLock::new(BTreeMap::new()));
    let users_clone = users.clone();

    let locations = Arc::new(RwLock::new(BTreeMap::new()));
    let locations_clone = locations.clone();

    let visits = Arc::new(RwLock::new(BTreeMap::new()));
    let visits_clone = visits.clone();

    let location_marks = Arc::new(RwLock::new(BTreeMap::new()));
    let location_marks_clone = location_marks.clone();

    let user_visits = Arc::new(RwLock::new(BTreeMap::new()));
    let user_visits_clone = user_visits.clone();

    fn show_memory_usage() {
        println!(
            "-----------------\n{}",
           String::from_utf8_lossy(&Command::new("free").arg("-m").output().expect("failed to execute process").stdout),
        );
    }

    show_memory_usage();

    // process entities
    thread::spawn(move || {
        let entities_load = Instant::now();
        let data_path = env::var("DATA_PATH").unwrap_or("/root".to_string());
        println!("Load entities from: {}", data_path);
        let paths = fs::read_dir(data_path.clone()).unwrap();
        let pool = ThreadPool::new(num_cpus::get());

        for path in paths {

            let filepath = path.unwrap().path().display().to_string();
            let users = users_clone.clone();
            let locations = locations_clone.clone();
            let location_marks = location_marks_clone.clone();
            let user_visits = user_visits_clone.clone();

            pool.execute(move || {
                let file = File::open(filepath.to_owned()).unwrap();

                if USER_FILE_RE.is_match(&filepath) {
                    let users_json: Users = serde_json::from_reader(&file).unwrap();
                    for user in users_json.users {
                        let user_visit_list = UserVisitList {
                            user: user.id,
                            visits: Vec::new(),
                        };
                        save_user_visits(user_visit_list, &user_visits);
                        save_user(user, &users);
                    }
                }
                if LOCATION_FILE_RE.is_match(&filepath) {
                    let locations_json: Locations = serde_json::from_reader(&file).unwrap();
                    for location in locations_json.locations {
                        let location_mark_list = LocationMarkList {
                            location: location.id,
                            marks: Vec::new(),
                        };
                        save_location_mark_list(location_mark_list, &location_marks);
                        save_location(location, &locations);
                    }
                }
            });
        }

        pool.join();

        let paths = fs::read_dir(data_path).unwrap();
        for path in paths {

            let user_visits = user_visits_clone.clone();
            let location_marks = location_marks_clone.clone();

            let users = users_clone.clone();
            let locations = locations_clone.clone();
            let visits = visits_clone.clone();

            let filepath = path.unwrap().path().display().to_string();
            pool.execute(move || {

                let file = File::open(filepath.to_owned()).unwrap();

                if VISITS_FILE_RE.is_match(&filepath) {
                    let visits_json: Visits = serde_json::from_reader(&file).unwrap();
                    for item in visits_json.visits {

                        // populate user visits
                        if let Some(mut visits) = load_user_visits(item.user, &user_visits) {
                            if let Some(location) = load_location(item.location, &locations) {
                                // construct body
                                let user_visit_body = UserVisitBody {
                                    visited_at: item.visited_at,
                                    mark: item.mark,
                                    place: location.place.to_owned(),
                                };

                                // construct index
                                let user_visit = UserVisit {
                                    user: item.user,
                                    visit: item.id,
                                    location: location.id,
                                    distance: location.distance,
                                    country: location.country.to_owned(),
                                    body: user_visit_body,
                                };

                                if let Some(index) = visits.visits.iter().position(|i| i.body.visited_at > item.visited_at) {
                                    // find position
                                    visits.visits.insert(index, user_visit);
                                } else {
                                    // add to the list
                                    visits.visits.push(user_visit);
                                }

                                save_user_visits(visits, &user_visits);
                            }
                        }

                        // populate location marsk
                        if let Some(mut marks) = load_location_mark_list(item.location, &location_marks) {
                            if let Some(user) = load_user(item.user, &users) {
                                marks.marks.push(LocationMark {
                                    user: user.id,
                                    visit: item.id,
                                    gender: match user.gender.as_ref() {
                                        "m" => Gender::MALE,
                                        _ => Gender::FEMALE,
                                    },
                                    birth_date: user.birth_date,
                                    mark: item.mark,
                                    visited_at: item.visited_at,
                                });
                                save_location_mark_list(marks, &location_marks);
                            }
                        }

                        save_visit(item, &visits);
                    }
                }
            });
        }

        pool.join();

        println!("Entities load done {:?}", entities_load.elapsed());
        show_memory_usage();
    });

    let listen_on = env::var("LISTEN").unwrap_or("0.0.0.0:80".to_string());
    let addr = listen_on.parse().unwrap();
    let mut srv = TcpServer::new(Http, addr);
    srv.threads(num_cpus::get());

    srv.serve(move || {
        let users = users.clone();
        let locations = locations.clone();
        let visits = visits.clone();
        let user_visits = user_visits.clone();
        let location_marks = location_marks.clone();

        Ok(Travels { users, locations, visits, user_visits, location_marks })
    })
}
