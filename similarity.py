from scipy import stats

import neo4jUtils as db


THRES = 0.5


def main():
    setAllSimilarities("anonymous100")

def setAllSimilarities(userName):
    "Sets similarity with all users with common liked movies"
    db.deleteSimilarities(userName)
    userLikes = db.getLikesCount(userName)
    others = db.getUsersWithCommonLikes(userName)

    while others.forward():
        otherID = others.current[0]
        common = float(others.current[1])
        otherLikes = float(others.current[2])
        similarity = stats.hmean([common/userLikes, common/otherLikes])

        if similarity >= THRES:
            db.updateSimilarity(userName, otherID, similarity)

if __name__ == '__main__':
    main()