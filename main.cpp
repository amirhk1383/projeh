// main.cpp
// Compile: g++ -O3 -fopenmp -std=c++14 main.cpp -o assign -lcurl

// -----------------------------------------------------------
// 1) All includes
// -----------------------------------------------------------
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <cmath>
#include <limits>
#include <iomanip>
#include <cstdint>
#include <sys/stat.h>    // For mkdir on Linux/Mac
#ifdef _WIN32
  #include <direct.h>    // For _mkdir on Windows
#endif
#include <curl/curl.h>
#include "json.hpp"
#include <omp.h>

using json = nlohmann::json;

// -----------------------------------------------------------
// 2) Constants and utility functions
// -----------------------------------------------------------
#ifndef M_PI
  #define M_PI 3.14159265358979323846
#endif

// KD-Tree structures and functions
struct Point2D { double x, y; size_t idx; };
struct KDNode {
    Point2D pt;
    KDNode *left, *right;
    KDNode(const Point2D& p) : pt(p), left(nullptr), right(nullptr) {}
};

KDNode* buildKD(std::vector<Point2D>& pts, int l, int r, int depth) {
    if (l >= r) return nullptr;
    int dim = depth & 1;
    int m = (l + r) >> 1;
    std::nth_element(pts.begin() + l, pts.begin() + m, pts.begin() + r,
        [dim](const Point2D& a, const Point2D& b) {
            return (dim == 0 ? a.x < b.x : a.y < b.y);
        });
    KDNode* node = new KDNode(pts[m]);
    node->left = buildKD(pts, l, m, depth + 1);
    node->right = buildKD(pts, m + 1, r, depth + 1);
    return node;
}

void nearestSearch(KDNode* node, double qx, double qy,
                   size_t& bestIdx, double& bestDist2, int depth) {
    if (!node) return;
    double dx = qx - node->pt.x, dy = qy - node->pt.y;
    double d2 = dx * dx + dy * dy;
    if (d2 < bestDist2) {
        bestDist2 = d2;
        bestIdx = node->pt.idx;
    }
    int dim = depth & 1;
    double diff = (dim == 0 ? dx : dy);
    KDNode *first = diff < 0 ? node->left : node->right;
    KDNode *second = diff < 0 ? node->right : node->left;
    nearestSearch(first, qx, qy, bestIdx, bestDist2, depth + 1);
    if (diff * diff < bestDist2)
        nearestSearch(second, qx, qy, bestIdx, bestDist2, depth + 1);
}

void freeKD(KDNode* node) {
    if (!node) return;
    freeKD(node->left);
    freeKD(node->right);
    delete node;
}

// Euclidean and Haversine distance functions
inline double euclidDist(double a, double b, double c, double d) {
    double dx = a - c, dy = b - d;
    return std::sqrt(dx * dx + dy * dy);
}

double haversineDist(double lat1, double lon1, double lat2, double lon2) {
    const double R = 6371000.0, toRad = M_PI / 180.0;
    double φ1 = lat1 * toRad, φ2 = lat2 * toRad;
    double dφ = (lat2 - lat1) * toRad, dλ = (lon2 - lon1) * toRad;
    double a = std::sin(dφ / 2) * std::sin(dφ / 2) +
               std::cos(φ1) * std::cos(φ2) * std::sin(dλ / 2) * std::sin(dλ / 2);
    return R * 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
}

// Callback for libcurl
static size_t WriteCallback(void* ptr, size_t sz, size_t nm, void* up) {
    ((std::string*)up)->append((char*)ptr, sz * nm);
    return sz * nm;
}

// Function to write HTML with Leaflet map
void writeLeafletMap(const std::string& filename,
                     const std::string& title,
                     const std::vector<std::pair<double, double>>& pts,
                     const std::vector<std::string>& labels,
                     std::pair<double, double> center = {30.4380, 56.8468},
                     int zoom = 12) {
    std::ofstream ofs(filename);
    ofs << R"(<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>)" << title
        << R"(</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="https://unpkg.com/leaflet/dist/leaflet.css"/>
  <script src="https://unpkg.com/leaflet/dist/leaflet.js"></script>
  <style>#map{height:90vh;width:100%;}</style>
</head><body>
  <h3>)" << title << R"(</h3>
  <div id="map"></div>
  <script>
    var map = L.map('map').setView([)" << center.first << "," << center.second
        << "]," << zoom << R"();
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{
      maxZoom:19
    }).addTo(map);
)";
    for (size_t i = 0; i < pts.size(); ++i) {
        ofs << "    L.marker([" << pts[i].first << "," << pts[i].second << "])";
        if (i < labels.size() && !labels[i].empty())
            ofs << ".bindPopup(\"" << labels[i] << "\")";
        ofs << ".addTo(map);\n";
    }
    ofs << R"(</script></body></html>)";
    ofs.close();
}

// -----------------------------------------------------------
// 3) Main function
// -----------------------------------------------------------
int main() {
    // Paths and parameters
    const std::string personPath = "D:/file/person.csv";
    const std::string khyPath = "D:/file/kheyaban.csv";
    const std::string outDir = "D:/c++/khroji/";
    const double schoolLat = 30.43803503943664;
    const double schoolLon = 56.84678321899874;

    // Read students
    std::vector<std::string> personID;
    std::vector<double> personLat, personLon;
    {
        std::ifstream ifs(personPath);
        if (!ifs.is_open()) {
            std::cerr << "Error: Cannot open " << personPath << std::endl;
            return 1;
        }
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string id, la, lo;
            std::getline(ss, id, ',');
            std::getline(ss, la, ',');
            std::getline(ss, lo, ',');
            personID.push_back(id);
            personLat.push_back(std::stod(la));
            personLon.push_back(std::stod(lo));
        }
    }
    size_t nPerson = personID.size();

    // Read stations
    std::vector<std::string> khyDesc, khyType;
    std::vector<double> khyLat, khyLon;
    std::vector<int64_t> khyID;
    {
        std::ifstream ifs(khyPath);
        if (!ifs.is_open()) {
            std::cerr << "Error: Cannot open " << khyPath << std::endl;
            return 1;
        }
        std::string line;
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string ds, la, lo, id, tp;
            std::getline(ss, ds, ','); khyDesc.push_back(ds);
            std::getline(ss, la, ','); khyLat.push_back(std::stod(la));
            std::getline(ss, lo, ','); khyLon.push_back(std::stod(lo));
            std::getline(ss, id, ','); khyID.push_back(std::stoll(id));
            std::getline(ss, tp, ','); khyType.push_back(tp);
        }
    }
    size_t nKhy = khyLat.size();

    // Stage 1: KD-Tree + Euclidean distance
    std::vector<Point2D> stPts;
    stPts.reserve(nKhy);
    for (size_t j = 0; j < nKhy; ++j)
        stPts.push_back({khyLat[j], khyLon[j], j});
    KDNode* root = buildKD(stPts, 0, static_cast<int>(stPts.size()), 0);

    std::vector<size_t> bestIdx(nPerson);
    std::vector<double> bestDist(nPerson);
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < static_cast<int>(nPerson); ++i) {
        double qx = personLat[i], qy = personLon[i];
        size_t bi = 0;
        double bd2 = 1e300;
        nearestSearch(root, qx, qy, bi, bd2, 0);
        bestIdx[i] = bi;
        bestDist[i] = std::sqrt(bd2);
    }
    {
        std::ofstream ofs(outDir + "student_initial_assignment.txt");
        ofs << "StudentID,AssignedLat,AssignedLon,EuclidDist\n";
        ofs << std::fixed << std::setprecision(6);
        for (size_t i = 0; i < nPerson; ++i) {
            size_t j = bestIdx[i];
            ofs << personID[i] << ','
                << khyLat[j] << ',' << khyLon[j] << ','
                << bestDist[i] << '\n';
        }
    }
    freeKD(root);

    // Stage 2/3: Haversine + OSRM
    std::ofstream ofsH(outDir + "student_haversine_valid.txt");
    std::ofstream ofsF(outDir + "final_student_assignment.txt");
    ofsH << "StudentID,CenterLat,CenterLon,NearbyID,NearbyLat,NearbyLon,Dist_m\n"
         << std::fixed << std::setprecision(6);
    ofsF << "StudentID,CenterLat,CenterLon,NearbyID,NearbyLat,NearbyLon,HavDist_m,OSRMDist_m\n"
         << std::fixed << std::setprecision(6);

    curl_global_init(CURL_GLOBAL_DEFAULT);
    std::unordered_map<int64_t, double> osrmCache;
    std::vector<int> osrmValidCount(nKhy, 0);
    std::vector<std::vector<std::pair<size_t, double>>> personCandidates(nPerson);

    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < static_cast<int>(nPerson); ++i) {
        std::string bufH, bufF;
        std::vector<std::pair<size_t, double>> localC;
        size_t ci = bestIdx[i];
        double clat = khyLat[ci], clon = khyLon[ci];

        CURL* curl = curl_easy_init();
        if (!curl) continue;
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);

        for (size_t j = 0; j < nKhy; ++j) {
            double h = haversineDist(clat, clon, khyLat[j], khyLon[j]);
            if (h > 500.0) continue;
            {
                std::ostringstream ss;
                ss << personID[i] << ',' << clat << ',' << clon << ','
                   << khyID[j] << ',' << khyLat[j] << ',' << khyLon[j]
                   << ',' << h << '\n';
                bufH += ss.str();
            }
            int64_t key = (static_cast<int64_t>(ci) << 32) | static_cast<int64_t>(j);
            double od = 0;
            bool found = false;
            #pragma omp critical
            {
                auto it = osrmCache.find(key);
                if (it != osrmCache.end()) {
                    od = it->second;
                    found = true;
                }
            }
            if (!found) {
                std::string rb;
                std::ostringstream url;
                url << "http://localhost:5000/route/v1/walking/"
                    << clon << ',' << clat << ';'
                    << khyLon[j] << ',' << khyLat[j]
                    << "?overview=false";
                curl_easy_setopt(curl, CURLOPT_URL, url.str().c_str());
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rb);
                if (curl_easy_perform(curl) == CURLE_OK) {
                    try {
                        auto js = json::parse(rb);
                        if (js["code"] == "Ok" && !js["routes"].empty())
                            od = js["routes"][0]["distance"].get<double>();
                        else
                            od = std::numeric_limits<double>::infinity();
                    } catch (...) {
                        od = std::numeric_limits<double>::infinity();
                    }
                } else {
                    od = std::numeric_limits<double>::infinity();
                }
                #pragma omp critical
                osrmCache[key] = od;
            }
            if (od <= 550.0) {
                std::ostringstream ss2;
                ss2 << personID[i] << ',' << clat << ',' << clon << ','
                    << khyID[j] << ',' << khyLat[j] << ',' << khyLon[j]
                    << ',' << h << ',' << od << '\n';
                bufF += ss2.str();
                localC.emplace_back(j, od);
                #pragma omp atomic
                osrmValidCount[j]++;
            }
        }
        curl_easy_cleanup(curl);

        #pragma omp critical
        {
            ofsH << bufH;
            ofsF << bufF;
            personCandidates[i] = std::move(localC);
        }
    }
    curl_global_cleanup();
    ofsH.close();
    ofsF.close();

    // Stage 4b: Greedy assignment with updated typePriority and OSRM distance to school
    const int MAX_PER_STATION_B = 15;
    std::vector<std::vector<std::pair<size_t, double>>> stationCandidates(nKhy);
    for (size_t i = 0; i < nPerson; ++i)
        for (const auto &pr : personCandidates[i])
            stationCandidates[pr.first].push_back(pr);
    for (size_t j = 0; j < nKhy; ++j)
        std::sort(stationCandidates[j].begin(), stationCandidates[j].end(),
                  [](const std::pair<size_t, double>& a, const std::pair<size_t, double>& b) {
                      return a.second < b.second;
                  });

    std::unordered_map<int64_t, int> osmRepeat;
    for (const auto &id : khyID) osmRepeat[id]++;
    std::map<std::string, int> typePriority = {
        {"track", 0}, {"trunk", 1}, {"trunk_link", 2}, {"primary", 3},
        {"primary_link", 4}, {"secondary", 5}, {"secondary_link", 6},
        {"tertiary", 7}, {"tertiary_link", 8}, {"residential", 9},
        {"unclassified", 10}
    };

    std::vector<std::string> paramNames = {"Count", "TypePriority", "OsmRepeat", "DistToSchool"};
    std::vector<std::vector<int>> permList = {
        {0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}, {3, 1, 2, 0}
    };

    // Calculate OSRM distance from each station to school
    std::vector<double> stationToSchoolDist(nKhy, 0.0);
    {
        CURL* curl = curl_easy_init();
        if (curl) {
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
            for (size_t j = 0; j < nKhy; ++j) {
                std::string rb;
                std::ostringstream url;
                url << "http://localhost:5001/route/v1/drive/"
                    << khyLon[j] << ',' << khyLat[j] << ';'
                    << schoolLon << ',' << schoolLat
                    << "?overview=false";
                curl_easy_setopt(curl, CURLOPT_URL, url.str().c_str());
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &rb);
                if (curl_easy_perform(curl) == CURLE_OK) {
                    try {
                        auto js = json::parse(rb);
                        if (js["code"] == "Ok" && !js["routes"].empty())
                            stationToSchoolDist[j] = js["routes"][0]["distance"].get<double>();
                        else
                            stationToSchoolDist[j] = std::numeric_limits<double>::infinity();
                    } catch (...) {
                        stationToSchoolDist[j] = std::numeric_limits<double>::infinity();
                    }
                } else {
                    stationToSchoolDist[j] = std::numeric_limits<double>::infinity();
                }
            }
            curl_easy_cleanup(curl);
        }
    }

    #pragma omp parallel for schedule(dynamic)
    for (int pi = 0; pi < static_cast<int>(permList.size()); ++pi) {
        const auto &perm = permList[pi];
        std::vector<bool> stuAssigned(nPerson, false), stSelected(nKhy, false);
        std::map<size_t, int> stationStudentCount;

        std::ostringstream fn;
        fn << outDir << "greedy_assignment_perm"
           << std::setw(2) << std::setfill('0') << (pi + 1) << ".txt";
        std::ofstream ofs(fn.str());

        ofs << "# PriorityOrder:";
        for (int k : perm) ofs << ' ' << paramNames[k];
        ofs << "\nStudentID,StationDesc,StationLat,StationLon,OSM_ID,Type,OrderInStation\n";

        struct SC {
            int count, typePr, osmRep;
            double dist;
            size_t idx;
        };

        while (true) {
            std::vector<SC> cands;
            for (size_t j = 0; j < nKhy; ++j) {
                if (stSelected[j]) continue;
                int cnt = 0;
                for (size_t i = 0; i < nPerson; ++i) {
                    if (stuAssigned[i]) continue;
                    for (const auto &pc : personCandidates[i])
                        if (pc.first == j) {
                            cnt++;
                            break;
                        }
                }
                if (cnt > 0) {
                    int tp = typePriority.count(khyType[j]) ? typePriority[khyType[j]] : 100;
                    cands.push_back({cnt, tp, osmRepeat[khyID[j]], stationToSchoolDist[j], j});
                }
            }
            if (cands.empty()) break;
            std::sort(cands.begin(), cands.end(),
                      [&perm](const SC& a, const SC& b) {
                          for (int k : perm) {
                              if (k == 0 && a.count != b.count) return a.count > b.count;
                              if (k == 1 && a.typePr != b.typePr) return a.typePr < b.typePr;
                              if (k == 2 && a.osmRep != b.osmRep) return a.osmRep > b.osmRep;
                              if (k == 3 && a.dist != b.dist) return a.dist < b.dist;
                          }
                          return a.idx < b.idx;
                      });
            size_t sel = cands.front().idx;
            stSelected[sel] = true;

            std::vector<std::pair<size_t, double>> studs;
            for (size_t i = 0; i < nPerson; ++i) {
                if (stuAssigned[i]) continue;
                for (const auto &pc : personCandidates[i]) {
                    if (pc.first == sel) {
                        studs.emplace_back(i, pc.second);
                        break;
                    }
                }
            }
            std::sort(studs.begin(), studs.end(),
                      [](const std::pair<size_t, double>& a, const std::pair<size_t, double>& b) {
                          return a.second < b.second;
                      });
            int toA = std::min(static_cast<int>(studs.size()), MAX_PER_STATION_B);
            stationStudentCount[sel] = toA;
            for (int k = 0; k < toA; ++k) {
                size_t stu = studs[k].first;
                stuAssigned[stu] = true;
                ofs << personID[stu] << ','
                    << khyDesc[sel] << ','
                    << khyLat[sel] << ',' << khyLon[sel] << ','
                    << khyID[sel] << ',' << khyType[sel] << ','
                    << (k + 1) << '\n';
            }
        }

        // Write CSV for selected stations
        std::ostringstream csvFn;
        csvFn << outDir << "station_summary_perm"
              << std::setw(2) << std::setfill('0') << (pi + 1) << ".csv";
        std::ofstream csvOfs(csvFn.str());
        csvOfs << "StationDesc,StudentCount,StationLon,StationLat\n";
        for (const auto& pair : stationStudentCount) {
            size_t j = pair.first;
            int count = pair.second;
            csvOfs << khyDesc[j] << ',' << count << ','
                   << khyLon[j] << ',' << khyLat[j] << '\n';
        }
        csvOfs.close();

        for (size_t i = 0; i < nPerson; ++i) {
            if (!stuAssigned[i]) {
                #pragma omp critical
                std::cerr << "Warning (perm " << (pi + 1) << "): Student " << personID[i]
                          << " not assigned\n";
            }
        }
        ofs.close();
    }

    // HTML generation
    std::string htmlDir = outDir + "html/";
#ifdef _WIN32
    _mkdir(htmlDir.c_str());
#else
    mkdir(htmlDir.c_str(), 0755);
#endif

    // Students map
    {
        std::vector<std::pair<double, double>> pts;
        std::vector<std::string> labs;
        pts.reserve(nPerson);
        labs.reserve(nPerson);
        for (size_t i = 0; i < nPerson; ++i) {
            pts.emplace_back(personLat[i], personLon[i]);
            labs.push_back(personID[i]);
        }
        writeLeafletMap(htmlDir + "students_map.html",
                        "Students Locations",
                        pts, labs,
                        {schoolLat, schoolLon}, 12);
    }

    // Stations map for each permutation
    for (int pi = 0; pi < 7; ++pi) {
        std::string fin = outDir + "greedy_assignment_perm" + std::to_string(pi + 1) + ".txt";
        std::string fout = htmlDir + "stations_perm" + std::to_string(pi + 1) + ".html";
        std::ifstream ifs(fin);
        if (!ifs.is_open()) {
            std::cerr << "Error: Cannot open " << fin << std::endl;
            continue;
        }
        std::string line;
        std::set<std::pair<double, double>> coords;
        std::getline(ifs, line); // Skip header
        while (std::getline(ifs, line)) {
            if (line.empty()) continue;
            std::stringstream ss(line);
            std::string stu, desc, ls, lo, dummy;
            std::getline(ss, stu, ',');
            std::getline(ss, desc, ',');
            std::getline(ss, ls, ',');
            std::getline(ss, lo, ',');
            std::getline(ss, dummy); // Skip remaining fields
            coords.emplace(std::stod(ls), std::stod(lo));
        }
        ifs.close();
        std::vector<std::pair<double, double>> pts;
        std::vector<std::string> labs;
        for (const auto &p : coords) {
            pts.push_back(p);
            labs.push_back("");
        }
        writeLeafletMap(fout,
                        "Stations perm " + std::to_string(pi + 1),
                        pts, labs,
                        {schoolLat, schoolLon}, 12);
    }

    // Index HTML
    {
        std::ofstream idx(htmlDir + "index.html");
        idx << R"(<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Maps Index</title></head><body>
  <h2>Download HTML Maps</h2>
  <ul>
    <li><a href="students_map.html">Students Locations Map</a></li>)";
        for (int i = 1; i <= 7; ++i) {
            idx << "    <li><a href=\"stations_perm"
                << std::setw(2) << std::setfill('0') << i
                << ".html\">Stations Map perm " << i << "</a></li>\n";
        }
        idx << R"(</ul></body></html>)";
        idx.close();
    }

    return 0;
}
