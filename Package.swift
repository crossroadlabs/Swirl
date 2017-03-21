import PackageDescription

let package = Package(
    name: "RDBC",
    targets: [
        Target(
            name: "RDBC"
        ),
	Target(
	    name: "RDBCSQLite",
	    dependencies: [.Target(name: "RDBC")]
	),
        Target(
                name: "Demo", 
                dependencies: [.Target(name: "RDBC"), .Target(name: "RDBCSQLite")]
        )
    ],
    dependencies: [
        .Package(url: "https://github.com/reactive-swift/Future.git", "0.2.0-alpha"),
        .Package(url: "https://github.com/IBM-Swift/CLibpq.git", majorVersion: 0, minor: 1),
        .Package(url: "https://github.com/carlbrown/CSQLite.git", majorVersion: 0, minor: 0),
    ]
)
