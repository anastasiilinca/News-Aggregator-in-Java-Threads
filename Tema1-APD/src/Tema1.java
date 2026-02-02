import com.fasterxml.jackson.databind.ObjectMapper;

import javax.lang.model.type.ArrayType;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;

import static java.lang.Math.pow;

public class Tema1 {
    static int NR_OF_THREADS = 0;
    static String articlesFile = "";
    static String extrasFile = "";
    static String categoriesFile = "";
    static String englishLinkingWordsFile = "";
    static String languageFile = "";
    static ArrayList<ArrayList<Article>> partialArticles = new ArrayList<>();
    static ArrayList<Article> articles = new ArrayList<>();
    static Article[] aux;
    static ArrayList<String> categories = new ArrayList<>();
    static ArrayList<String> languages = new ArrayList<>();
    static ArrayList<String> englishLinkingWords = new ArrayList<>();
    static ArrayList<HashMap<String, Integer>> partialArticleByTitle;
    static ArrayList<HashMap<String, Integer>> partialArticleByUuid;
    static ConcurrentHashMap<String, Integer> articleFreqByTitle = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, Integer> articleFreqByUuid = new ConcurrentHashMap<String, Integer>();
    static CyclicBarrier barrier;
    /* Hashmap with <language, list of all articles in that language> format. */
    static ArrayList<HashMap<String, ArrayList<String>>> partialArticlesByLanguage;
    static ArrayList<HashMap<String, ArrayList<String>>> partialArticlesByCategory;
    static ConcurrentHashMap<String, ArrayList<String>> articlesByLanguage = new ConcurrentHashMap<>();
    /* Create HashMap with <category, list of related articles> format. */
    static ConcurrentHashMap<String, ArrayList<String>> articlesByCategory = new ConcurrentHashMap<>();

    /* Vars needed for TASK6 - Reports. */
    static int nrOfDuplicates = 0;
    static int nrOfUniqueArticles = 0;
    /* best author */
    static ArrayList<HashMap<String, Integer>> partialAuthorFreq = new ArrayList<>();
    static HashMap<String, Integer> authorFreq = new HashMap<>();
    static String bestAuthor = null;
    static int bestAuthorCount = 0;
    /* Top language */
    static ConcurrentHashMap<String, Integer> languageFreq = new ConcurrentHashMap<>();
    static String topLanguage = null;
    static int topLanguageCount = 0;
    /* Top category */
    static ConcurrentHashMap<String, Integer> categoryFreq = new ConcurrentHashMap<>();
    static String topCategory = null;
    static int topCategoryCount = 0;
    /* Top english key word */
    static ArrayList<HashMap<String, Integer>> partialWordCount = new ArrayList<>();
    static ConcurrentHashMap<String, Integer> globalWordCount = new ConcurrentHashMap<>();
    static ArrayList<Map.Entry<String, Integer>> wordCount = new ArrayList<>();
    static ArrayList<Article> englishArticles = new ArrayList<>();
    static ArrayList<ArrayList<Article>> partialEnglishArticles = new ArrayList<>();
    static String topEnglishWord = null;
    static int topEnglishWordCount = 0;
    /* Most recent article. */
    static ArrayList<Article> mostRecentArticleFromThreads;
    static Article mostRecentArticle = null;
    static final Object lock = new Object();
    static int initialNrOfArticles;
    static ArrayList<String> fileNames = new ArrayList<>();
    static ObjectMapper mapper;
    static String helperPath;

    public static void main(String[] args) {

        /* Check if all CLI arguments were given. */
        if (args.length < 3) {
            System.out.println("Usage: java Tema1 <number of threads> <articles file> <extras file>");
            System.exit(0);
        } else {
            NR_OF_THREADS = Integer.parseInt(args[0]);
            articlesFile = args[1];
            extrasFile = args[2];
        }

        /* Helper path for articles. */
        helperPath = articlesFile.substring(0, articlesFile.lastIndexOf('/') + 1);

        /* Now that NR_OF_THREADS has been read, initialize CyclicBarrier. */
        barrier = new CyclicBarrier(NR_OF_THREADS);

        /* Instantiate parser */
        mapper = new ObjectMapper();

        /* Instantiate mostRecentArticleFromThreads with NR_OF_THREADS size. */
        mostRecentArticleFromThreads = new ArrayList<>(NR_OF_THREADS);


        for (int i = 0; i < NR_OF_THREADS; i++) {
            mostRecentArticleFromThreads.add(null);
        }

        /* Instantiate partialAuthorFreq. */
        partialAuthorFreq = new ArrayList<>(NR_OF_THREADS);

        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialAuthorFreq.add(new HashMap<>());
        }

        /* Instantiate partial word count. */
        partialWordCount = new ArrayList<>(NR_OF_THREADS);

        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialWordCount.add(new HashMap<>());
        }

        partialArticlesByLanguage = new ArrayList<>(NR_OF_THREADS);
        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialArticlesByLanguage.add(new HashMap<>());
        }

        partialArticlesByCategory = new ArrayList<>(NR_OF_THREADS);
        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialArticlesByCategory.add(new HashMap<>());
        }

        partialArticleByTitle = new ArrayList<>(NR_OF_THREADS);

        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialArticleByTitle.add(new HashMap<>());
        }

        partialArticleByUuid = new ArrayList<>(NR_OF_THREADS);

        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialArticleByUuid.add(new HashMap<>());
        }

        partialEnglishArticles = new ArrayList<>(NR_OF_THREADS);

        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialEnglishArticles.add(new ArrayList<>());
        }

        partialArticles = new ArrayList<>(NR_OF_THREADS);

        for (int i = 0; i < NR_OF_THREADS; i++) {
            partialArticles.add(new ArrayList<>());
        }

        /* Read articles. */
        try {
            BufferedReader br = new BufferedReader(new FileReader(articlesFile));

            int n = Integer.parseInt(br.readLine().trim());

            for (int i = 0; i < n; i++) {
                String fileName = br.readLine().trim();
                fileNames.add(fileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        /* Read extra files. */
        try {
            BufferedReader br = new BufferedReader(new FileReader(extrasFile));

            int n = Integer.parseInt(br.readLine().trim());

            languageFile = br.readLine().trim();
            categoriesFile = br.readLine().trim();
            englishLinkingWordsFile = br.readLine().trim();

            /* Read categories. */
            br = new BufferedReader(new FileReader(helperPath + categoriesFile));

            String line;
            line = br.readLine();

            while ((line = br.readLine()) != null) {
                categories.add(line);
            }

            /* Read languages. */
            br = new BufferedReader(new FileReader(helperPath + languageFile));
            line = br.readLine();

            while ((line = br.readLine()) != null) {
                languages.add(line);
            }

            /* Read linking words. */
            br = new BufferedReader(new FileReader(helperPath + englishLinkingWordsFile));
            line = br.readLine();

            while ((line = br.readLine()) != null) {
                line = line.toLowerCase().replaceAll("[^a-z]", "");
                if (!line.isEmpty()) {
                    englishLinkingWords.add(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        /* Initializations. */
        for (String category : categories) {
            articlesByCategory.putIfAbsent(category, new ArrayList<>());
        }

        for (String language : languages) {
            articlesByLanguage.putIfAbsent(language, new ArrayList<>());
        }

        for (int i = 0; i < NR_OF_THREADS; i++) {
            for (String category : categories) {
                partialArticlesByCategory.get(i).putIfAbsent(category, new ArrayList<>());
            }

            for (String language : languages) {
                partialArticlesByLanguage.get(i).putIfAbsent(language, new ArrayList<>());
            }
        }

        /* Create threads. */
        MyThread[] threads = new MyThread[NR_OF_THREADS];

        for (int i = 0; i < NR_OF_THREADS; i++) {
            threads[i] = new MyThread(i);
        }

        /* Start threads. */
        for (int i = 0; i < NR_OF_THREADS; i++) {
            threads[i].start();
        }

        /* Wait threads to finish execution. */
        for (int i = 0; i < NR_OF_THREADS; i++) {
            try {
                threads[i].join();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.exit(0);
    }
}