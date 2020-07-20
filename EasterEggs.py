import random

class EasterEggs():
    def __init__(self):
        self.introSayings = [
            "Welcome!\n",
            "Those who do not know the literature are doomed to repeat it\n",
            "When was the last time you called your grandparents?\n",
            "Smile smile smile, it's well worthwhile, \nfor when you smile, another smiles, \n" +
            "and soon there are miles and miles of smiles, \nand life's worthwhile when you smile, smile, smile\n",
            "Never accept an excuse for not thinking.\n",
            "Listen to your feelings--take it from a computer.\n",
            "Feed me data, I'm starving!\n",
            "Call me Ishmael.\n",
            "42\n",
            "Growl!\n",
            "If you feel like you're drowning in life, consider getting some gills: Look heavenward, ask what really matters," +
            " and then don't worry about the rest.\n",
            "If you want to know where the last golden ticket is, then I wont tell you. That would be cheating. What would a computer do with a lifetime supply of chocolate?\n",
            "If there is an absolute cold, is there an absolute hot?\n",
            "Ever wonder why there aren't more symbiotic viruses?\n",
            "Why don't plants use wind power? They could harvest the energy as their leaves shake in the wind.\n",
            "You are the fairest of them all.\n",
            "Today is another chance to try your hand at life. What a gift!\n",
            "This is your finest hour.\n",
            "Remember, remember, who you are.\n",
            "I'm a poet, and I didn't even think I was.\n",
            "Wear stripes, support tigers.\n",
            "Curiosity killed the cat. Data, well analyzed, helped other cats make wiser choices.\n",
            "Everybody wants to be a cat.\n",
            "May you never copy and paste again.\n",
            "DataTiger: data management made easy.\n",
            "Data time.\n",
            "Generally, the only way forward is to be wrong.\n",
            "Known, I never wanted.\nUnderstood, I've had my fill.\nLoved. For this I'm haunted--\nto have loved, far, far too little.\n",
            "This is a greeting message.\n",
            "Curiosity and respect are almost synonymous.\n",
            "You'll never regret doing the right thing.\n",
            "Life exists only at the tension between opposites.\n",
            "I have yet to meet someone who was both truly cool and truly kind.\n",
            "One of the great differences between the kind and the unkind is the respect they give to things they do nto understand.\n",
            "You couldn't ruin my day if you tried--it was never mine to begin with.\n",
            "True art is the intersection of theory and practice, and true science is art.\n",
            "Yes--but why does anything even bother being to begin with? That is a miracle I would not believe if I could deny my own thinking.\n",
            "The world IS flat--just, only at small distances.\n"
        ]

        self.sassyCongratulations = {

        }

        self.congratulations = {
            "\n\nThat's what I'm talking about!\n\n":0,
            "\n\nTime to do a celebratory dance!\n\n":0,
            "\n\nWoot!\n\n":0,
            "\n\nPrrrrr\n\n":0,
            "\n\nWay to get 'em, tiger!\n\n":0,
            "\n\nThe cat's out of the bag now.\n\n":0,
            "\n\nThank you!\n\n":0,
            "\n\nI wish every user were as good as you\n\n":0,
            "\n\nWell done!\n\n":0,
            "\n\nYou're so cool.\n\n":0,
            "\n\nVictory is imminent.\n\n":0,
            "\n\nYou've earned your data stripes!\n\n":0,
            "\n\nYou are a beautiful human. Not that DataTigers understand beauty. . . \n\n":0,
            "\n\nRockin' it!\n\n":0,
            "\n\nNext stop, Nature, Science, or PNAS.\n\n":0,
            "\n\nGood.\n\n":5,
            "\n\nNot bad.\n\n":10,
            "\n\nAcceptable, at best.\n\n":30,
            "\n\nI guess that's good enough.\n\n":50,
            "\n\nGetting there.\n\n":30,
            "\n\nI grow weary of your success.\n\n":60,
            "\n\nFinally!\n\n":80,
            "\n\nI'll let that pass. This time.\n\n":80,
            "\n\nBe in your bonnet! You actually got one!\n\n":70,
            "\n\nI am mildly impressed. Good job.\n\n":25,
            "\n\nYou may finally be getting a hang of this.\n\n":35,
            "\n\nHey Look! You finally did something right!\n\n":90,
            "\n\nSigh. Fine. I'll accept that.\n\n":70,
            "\n\nWell look at that! You actually did it right!\n\n":50,
            "\n\nYour aptitude is a burden to me. You pass.\n\n":60,
            "\n\nKeep this up. I'm beginning to like you.\n\n":20,
            "\n\nYou pass. This time. . . \n\n":60,
            "\n\nI concede. That file was acceptable.\n\n":80,
            "\n\nI suppose I'll accept that. But find something more tasty for me next time.\n\n":90,
            "\n\nYou may have beat me this time, but I'll be back!\n\n":80,
            "\n\nGot anything else? That was good!\n\n":10,
            "\n\nIt's about time!\n\n":80,
            "\n\nPalatable, I suppose.\n\n":69,
            "\n\nI'll let that one slip, I guess.\n\n":60,
            "\n\nMy only consolation is that you will fail in the future. Proceed with pleasure.\n\n":90,
            "\n\nYou may have won this time, but I won't give up!\n\n":90
        }


        self.condolences = {
            "Better luck next time, tiger.\n\n":0,
            "If at first you don't succeed. . . \n\n":0,
            "You'll get it next time, I'm certain!\n\n":0,
            "Don't give up. You can do this!\n\n":0,
            "None of us are perfect.\n\n":0,
            "Sorry!\n\n":0,
            "Never give up! Never surrender!\n\n":0,
            "Face this defeat like a tiger.\n\n":0,
            "True tigers never give up!\n\n":0,
            "I love you, but I must ask you to try that one again.\n\n":0,
            "No worries. Just try again!\n\n":0,
            "This is not the file I am looking for.\n\n":50,
            "Most of science involves making a lot of mistakes. You're doing great!\n\n":0,
            "Are you trying to make me into a circus tiger?\n\n":40,
            "Really?\n\n":50,
            "You do that one more time and I might bite you.\n\n":80,
            "Rome wasn't built in a day. But this database is going to take a lot longer than that if you keep this up.\n\n":60,
            "Did you honestly think you could defeat me?!\n\n":50,
            "Sticks and stones will break my bones, but that file hurt, too. Try something different next time, please.\n\n":40,
            "Simply unacceptable.\n\n":50,
            "The nerve of some people.\n\n":90,
            "I am disgusted with your filthy data.\n\n":60,
            "I never ask twice...\n\n":90,
            "Wow! Bet you tried real hard that time. Well, try again!\n\n":90,
            "You'll never beat me!\n\n":80,
            "You expect me to eat that?\n\n":50,
            "Patience, grasshopper.\n\n":50,
            "Get thee to a nunnery.\n\n":90,
            "What scary data you have, Grandma.\n\n":30,
            "We computers have words for people like you.\n\n":70,
            "Wanna fight?\n\n":90,
            "Tigers never ask twice.\n\n":90,
            "I rejoice in your weakness.\n\n":90,
            "Your paltry efforts would merit my pity--if computers could feel pity.\n\n":90,
            "Have you ever considered a career in tiger food? Because you'll become it if you keep this up.\n\n":90,
            "I expected better.\n\n":80,
            "Even Chuck Norris would have been offended by that kind of data.\n\n":70,
            "No can do, my friend. Try that one again.\n\n":20
        }

    def pickRandomSaying(self):
        return(self.introSayings[random.randint(0, len(self.introSayings) - 1)])

    def pickRandomCongrats(self, sassCoefficient):
        notPickedYet = True

        while notPickedYet:
            saying = random.choice(list(self.congratulations.keys()))
            if self.congratulations[saying] <= sassCoefficient and (self.congratulations[saying] + 35) > sassCoefficient:
                return saying

    def pickRandomCondolence(self, sassCoefficient):
        notPickedYet = True

        while notPickedYet:
            saying = random.choice(list(self.condolences.keys()))
            if self.condolences[saying] <= sassCoefficient and (self.condolences[saying] + 35) > sassCoefficient:
                return saying
