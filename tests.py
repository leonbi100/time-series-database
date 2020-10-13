from db import TSDB, Sample
import unittest
import random

class TestTSDB(unittest.TestCase):
    def test_simple(self):
        '''
        This test checks that a small group of samples that are stored
        in the database are correctly queried and returned by the query
        method.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(10):
            samples.append(Sample(i, i))

        db.persist(samples)
        result = db.query(0, 10)
        
        self.assertEqual(result, samples)


    def test_large_samples(self):
        '''
        This test ensures that the database supports persistence
        up to 10k samples.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(10000):
            samples.append(Sample(i, i))

        db.persist(samples)
        result1 = db.query(0, 100)
        result2 = db.query(100, 200)

        self.assertEqual(len(result1), 100)
        self.assertEqual(len(result2), 100)
        self.assertEqual(result1, samples[:100])
        self.assertEqual(result2, samples[100:200])


    def test_query_large_multiple_time(self):
        '''
        This test ensures that the query returns at most 100 samples
        from a variety of times.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(10000):
            samples.append(Sample(i, i))

        db.persist(samples)
        result = db.query(0, 1000)

        self.assertEqual(len(result), 100)
        self.assertEqual(result, samples[:100])


    def test_query_large_single_time(self):
        '''
        This test ensures that the query returns at most 100 samples
        from a single time.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(10000):
            samples.append(Sample(0, i))

        db.persist(samples)
        result = db.query(0, 1)

        self.assertEqual(len(result), 100)
        self.assertEqual(result, samples[:100])

    def test_large_sample_values(self):
        '''
        This test ensures that the database supports large values
        for each sample.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(100):
            samples.append(Sample(i, 'A'*1000))

        db.persist(samples)
        result = db.query(0, 100)

        self.assertEqual(len(result), 100)
        self.assertEqual(result, samples[:100])
    
    def test_nonsequential_samples(self):
        '''
        This test ensures that the database supports non sequential
        insertion of samples.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(100):
            samples.append(Sample(random.randint(0, 99), f'TEST{i}'*10))
        db.persist(samples)
        result = db.query(0, 100)

        self.assertEqual(len(result), 100)
        # Need to sort to correctly check comparison of each sample
        self.assertEqual(result.sort(key=lambda x: x.time), samples[:100].sort(key=lambda x: x.time))

    def test_multiple_persistence(self):
        '''
        This test ensures that the database supports multiple calls
        to the persistence method.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(100):
            samples.append(Sample(i, i))
            
        db.persist(samples[:25])
        db.persist(samples[25:50])
        db.persist(samples[50:75])
        db.persist(samples[75:])
        result = db.query(0, 100)

        self.assertEqual(len(result), 100)
        self.assertEqual(result, samples)


    def test_multiple_queries(self):
        '''
        This test ensures that the database supports multiple calls
        to the query method.
        '''
        db = TSDB()
        db.clear()

        samples = []
        for i in range(100):
            samples.append(Sample(i, i))
            
        db.persist(samples)
        result1 = db.query(0, 25)
        result2 = db.query(25, 50)
        result3 = db.query(50, 75)
        result4 = db.query(75, 100)

        final = result1 + result2 + result3 + result4
        self.assertEqual(len(final), 100)
        self.assertEqual(final, samples)

if __name__ == '__main__':
    unittest.main()