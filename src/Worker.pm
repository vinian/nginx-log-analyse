package Worker;

use strict;
use warnings;

use Smart::Comments;
use POSIX qw(strftime);
use DBI;

# db table column map to log 
my $db2log = {
    mac         => 'ck_did',
    accessTime  => 'time',
    deviceId    => 'ck_platform',
    appVersion  => 'version',
    bookId      => 'bid',
    status      => 'status',
    height      => 'ck_height',
    width       => 'ck_width',
    entranceId  => 'entranceId',
    productId   => 'pid',
    likeStatus  => 'status',
    buyStatus   => 'status',
    openStatus  => 'status',
    readStatus  => 'status',
    loginStatus => 'status',
    loginTime   => 'time',
    buyTime     => 'time',
    userId      => 'user_id',
    downloadClickTime  => 'time',
    collectClickTime  => 'time',
    collectStatus      => 'status',
    downloadStatus     => 'status',
    loginSuccessStatus => 'status',
    len      => 'length',
    first    => 'first',
    page     => 'page',
    type     => 'type',
    row      => 'row',
    col      => 'col',
    cp       => 'cp',
    ep       => 'ep',
    IP       => 'client_ip',
};

sub new {
    my ($class, @args) = @_;

    my $self = {};

    $self->{51} = [ qw(user_collect_action mac collectClickTime bookId collectStatus) ];
    $self->{52} = [ qw(user_like_action mac accessTime appVersion bookId likeStatus) ];
    $self->{53} = [ qw(user_download_action mac bookId downloadClickTime downloadStatus) ];
    $self->{54} = [ qw(user_login_action mac loginTime loginStatus) ];
    $self->{55} = [ qw(user_buy_action buyTime bookId productId) ];
    $self->{56} = [ qw(location_info mac accessTime appVersion page type row col) ];
    $self->{57} = [ qw(user_read_action mac accessTime appVersion bookId cp ep) ];
    $self->{58} = [ qw(user_active_action mac accessTime appVersion first) ];
    # general:  ebook_access_info
    $self->{1000} = [ qw(ebook_access_info IP mac deviceId appVersion accessTime entranceId height width) ];

    bless $self, $class;
    $self->init();

    return $self;
}

sub init {
    my ($self) = @_;

    my $date = strftime("%Y-%m-%d", localtime(time-86400));
    $self->{log_format} = generate_log_format();
    $self->{log_file} = "/path/to/log";
    $self->{dbh} = connect_database();

    foreach my $pro ( 51..58  ) {
        $self->{"${pro}_sql"} = gen_sql( $pro, $self->{$pro} );
    }

    $self->{"1000_sql"} = gen_sql( 1000, $self->{1000} );
}

sub gen_sql {
    my ($pro, $column) = @_;

    my $table = shift @$column;
    my $keys = join ',', @$column;
    my $values = join ',', map { '?' } @$column;
    my $sql = qq{INSERT INTO $table ($keys) VALUES ($values)};

    return $sql;
}

sub open_file {
    my $self = shift;

    open my $fh, '<', $self->{log_file}
        or warn "Can't open $self->{log_file}: $!";

    $self->{fh} = $fh;
}

sub close_file {
    my $self = shift;

    close $self->{fh};
}

sub read_line {
    my $self = shift;

    my $fh = $self->{fh};
    return <$fh>;
}

sub insert_log {
    my ($self, $protocol,  $opt) = @_;

    my $dbh = $self->{dbh};

    my $sth = $dbh->prepare( $self->{"${protocol}_sql"} );

    my @values = map { $opt->{ $db2log->{$_} } } @{ $self->{$protocol} };
    ### $protocol
    ### $opt
    ### 118: $self->{"${protocol}_sql"}
    ### @values
    $sth->execute( @values );
    if ( $DBI::errstr ) {
        croak $DBI::errstr;
    }
}

sub generate_log_format {
    # '$remote_addr - $remote_user $time_custom "$request" $status $body_bytes_sent $http_cookie';
    my $regex = qr(^
                      (\S+)                     # remote_addr
                      \s+
                      -                         # -
                      \s+
                      (?:\S+)                   # remoate_user_name
                      \s+
                      (\d{4}-\d{2}-\d{2})       # date
                      \s+
                      (\d{2}:\d{2}:\d{2})       # time
                      \s+
                      "([^"]+)"                 # request
                      \s+
                      (\S+)                     # status
                      \s+
                      (\S+)                     # body bytes send
                      \s+
                      (.*)                      # cookie
              $)x;

    return $regex;
}

sub connect_database {
    my $dbinfo = {
        ip   => 'db-host',
        port => 3306,
        user => 'db-user',
        pass => 'db-pass'
        db   => 'db-name',
    };

    my $dbhost = $dbinfo->{ip};
    my $dbport = $dbinfo->{port};
    my $dbuser = $dbinfo->{user};
    my $dbpass = $dbinfo->{pass};
    my $dbname = $dbinfo->{db};

    my $db     = "DBI:mysql:$dbname;host=$dbhost";

    my $dbh;
    while ( 1 ) {
        $dbh = DBI->connect( $db, $dbuser, $dbpass,
                               {
                                   RaiseError => 1,
                               }
                           );
        if ( $DBI::errstr ) {
            sleep 10;
            next;
        }
        last;
    }

    return $dbh;
}

1;

